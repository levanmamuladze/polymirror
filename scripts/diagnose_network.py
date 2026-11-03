"""Diagnose Polymarket API connectivity, especially TLS failures.

Run this when the engine logs SSL/certificate errors:

    python scripts/diagnose_network.py

For every API host it reports OK / the exact failure, and on certificate
errors it additionally connects WITHOUT verification (diagnostic only —
nothing is trusted, only the certificate is read) to show WHO issued the
certificate being presented. An antivirus / VPN / corporate proxy that
inspects HTTPS shows up right there in the issuer name.

Verdicts:
  * ALL hosts fail with CERTIFICATE_VERIFY_FAILED
      -> something on this machine intercepts HTTPS. Fix (pick one):
         - pip install -r requirements.txt  (adds `truststore`; run.py then
           trusts the Windows certificate store, where the interceptor's
           root CA already lives) — recommended
         - or disable "HTTPS scanning / SSL inspection" in your antivirus
         - or set SSL_CERT_FILE to a CA bundle that includes the
           interceptor's root certificate
  * ONLY lb-api fails
      -> the leaderboard host itself is presenting a bad certificate
         (endpoint moved or is fronted differently). Open the URL in a
         browser: if the browser warns too, the endpoint is dead — set
         `scout.candidate_wallets` in config.yaml as an interim source and
         report the issue.

Read-only: every request is an unauthenticated GET.
"""
from __future__ import annotations

import asyncio
import ssl
import socket
import sys
from urllib.parse import urlsplit

import httpx

# Windows consoles often default to a legacy codepage; server strings can
# contain anything, and a diagnostic must never crash while printing.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOSTS = {
    "data-api (activity feed)":
        "https://data-api.polymarket.com/value?user=0x0000000000000000000000000000000000000000",
    "clob (prices)":
        "https://clob.polymarket.com/midpoint?token_id=1",
    "gamma (market metadata)":
        "https://gamma-api.polymarket.com/markets?limit=1",
    "lb-api (leaderboard / scout)":
        "https://lb-api.polymarket.com/leaderboard?window=1m&rankType=pnl&limit=1",
}


def peer_cert_issuer(host: str, port: int = 443) -> str:
    """Fetch the certificate the server presents, WITHOUT trusting it, and
    return its issuer — this is diagnostic-only and sends no data."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with socket.create_connection((host, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as tls:
                der = tls.getpeercert(binary_form=True)
        if not der:
            return "no certificate presented"
        # Best-effort issuer decode without extra dependencies: look for
        # printable issuer strings in the DER bytes.
        import re
        text = der.decode("latin-1", errors="replace")
        names = re.findall(r"[\w .,'()&/-]{4,64}", text)
        interesting = [n.strip() for n in names
                       if any(k in n.lower() for k in
                              ("ca", "cert", "secure", "root", "proxy", "gateway",
                               "kaspersky", "avast", "eset", "avg", "bitdefender",
                               "zscaler", "fortinet", "netskope", "cloudflare",
                               "let's encrypt", "digicert", "google", "amazon"))]
        return "; ".join(dict.fromkeys(interesting))[:200] or "unrecognized issuer (self-signed?)"
    except Exception as err:  # noqa: BLE001 — diagnostics report, never crash
        return f"could not read certificate: {err}"


def truststore_context() -> ssl.SSLContext | None:
    """The SSL context run.py uses when `truststore` is installed: full
    verification against the OPERATING SYSTEM certificate store."""
    try:
        import truststore
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except ImportError:
        return None


async def check(name: str, url: str) -> tuple[str, bool, str]:
    # Pass 1 — Python's bundled CA list (what this script's plain httpx uses).
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
        return name, True, f"OK — HTTP {r.status_code}"
    except httpx.ConnectError as err:
        msg = str(err)
        if not ("CERTIFICATE_VERIFY_FAILED" in msg or "certificate" in msg.lower()):
            return name, False, f"CONNECT FAILED — {msg[:160]}"
    except Exception as err:  # noqa: BLE001
        return name, False, f"FAILED — {type(err).__name__}: {str(err)[:160]}"

    detail = [f"TLS FAILED with Python's bundled CAs"]
    host = urlsplit(url).hostname or ""
    detail.append(f"presented cert mentions: {await asyncio.to_thread(peer_cert_issuer, host)}")

    # Pass 2 — the Windows certificate store (what run.py uses via truststore).
    ctx = truststore_context()
    if ctx is None:
        detail.append("truststore NOT installed -> cannot test the Windows store; "
                      "pip install -r requirements.txt and re-run")
    else:
        try:
            async with httpx.AsyncClient(timeout=15, verify=ctx) as client:
                r = await client.get(url)
            detail.append(f"BUT the Windows cert store trusts it (HTTP {r.status_code}) "
                          "-> run.py WILL work; just restart it")
            return name, True, "\n      ".join(detail)
        except Exception as err:  # noqa: BLE001
            detail.append(f"Windows cert store ALSO rejects it ({type(err).__name__})")

    # Pass 3 — diagnostic-only unverified fetch: what does the interceptor
    # actually return? A block page means the NETWORK forbids Polymarket.
    try:
        async with httpx.AsyncClient(timeout=15, verify=False, follow_redirects=True) as client:
            r = await client.get(url)
        body = r.text[:120].replace("\n", " ")
        detail.append(f"behind the interception the response is HTTP {r.status_code}: {body!r}")
        if r.status_code in (403, 451):
            detail.append(">> that is a BLOCK PAGE — this network/region refuses Polymarket; "
                          "no certificate fix can help")
    except Exception as err:  # noqa: BLE001
        detail.append(f"unverified fetch also failed: {type(err).__name__}")
    return name, False, "\n      ".join(detail)


async def main() -> int:
    print("Polymirror network diagnostic (read-only GETs)\n" + "=" * 60)
    results = await asyncio.gather(*(check(n, u) for n, u in HOSTS.items()))
    for name, ok, detail in results:
        print(f"{'[ OK ]' if ok else '[FAIL]'} {name}\n      {detail}")

    failures = [r for r in results if not r[1]]
    tls_failures = [r for r in failures if "TLS FAILED" in r[2]]
    print("\nVerdict\n" + "-" * 60)
    if not failures:
        print("All hosts reachable — the scout should work. If it still logs")
        print("errors, restart the engine and check data/polymirror.log.")
        return 0
    if len(tls_failures) == len(HOSTS):
        print("EVERY host fails TLS -> something between this machine and")
        print("Polymarket intercepts HTTPS. Read the per-host lines above:")
        print("  * 'Windows cert store trusts it' -> antivirus/VPN interception;")
        print("    run.py handles it via truststore — just restart the engine.")
        print("  * 'BLOCK PAGE' (HTTP 403/451)   -> this network or region")
        print("    refuses Polymarket entirely; no certificate fix can help.")
        print("  * Windows store also rejects, no block page -> unknown proxy;")
        print("    check VPN/proxy settings, or try another network to compare.")
    elif any("lb-api" in r[0] for r in failures) and len(failures) == 1:
        print("Only the leaderboard host fails -> open this URL in a browser:")
        print("  https://lb-api.polymarket.com/leaderboard?window=1m&rankType=pnl&limit=3")
        print("If the browser ALSO warns about the certificate, the endpoint is")
        print("dead/moved: set scout.candidate_wallets in config.yaml as an")
        print("interim candidate source. If the browser shows JSON, the")
        print("problem is local interception of Python only — install")
        print("requirements (truststore) and restart.")
    else:
        print("Mixed failures — likely no/blocked internet for this process.")
        print("Check firewall rules for Python, VPN split-tunneling, or proxy")
        print("environment variables (HTTP_PROXY/HTTPS_PROXY).")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
