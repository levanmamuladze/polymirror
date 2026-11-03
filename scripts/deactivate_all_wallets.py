from polymirror.storage import Store

DB = "data/polymirror.db"

if __name__ == "__main__":
    s = Store(DB)
    wallets = s.wallets()
    print(f"Found {len(wallets)} followed wallets (including inactive).")
    count = 0
    for w in wallets:
        if int(w.get("active", 1)) == 1:
            addr = w["address"]
            s.remove_wallet(addr)
            s.log_event('info', 'system', f'Wallet {addr} deactivated by user request')
            print(f"Deactivated: {addr}")
            count += 1
    s.close()
    print(f"Deactivated {count} wallets.")
