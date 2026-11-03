import pytest

from polymirror.sizing import SizingContext, target_notional


def ctx(**kw):
    base = dict(equity=10_000, available_cash=8_000,
                source_trade_usdc=5_000, source_portfolio_value=500_000,
                starting_balance=10_000)
    base.update(kw)
    return SizingContext(**base)


def test_proportional_scales_by_relative_portfolio_size():
    # Source risks 1% of a $500k book -> we risk 1% of $10k = $100.
    assert target_notional("proportional", 0, 1.0, ctx()) == pytest.approx(100.0)


def test_proportional_with_multiplier():
    assert target_notional("proportional", 0, 2.0, ctx()) == pytest.approx(200.0)


def test_proportional_zero_source_value_returns_zero():
    assert target_notional("proportional", 0, 1.0, ctx(source_portfolio_value=0)) == 0.0


def test_fixed_usd():
    assert target_notional("fixed_usd", 50, 1.0, ctx()) == 50.0
    assert target_notional("fixed_usd", 50, 0.5, ctx()) == 25.0


def test_percent_balance():
    assert target_notional("percent_balance", 0.05, 1.0, ctx()) == pytest.approx(400.0)


def test_capped_at_available_cash():
    assert target_notional("fixed_usd", 99_999, 1.0, ctx(available_cash=123.0)) == 123.0


def test_unknown_mode_raises():
    with pytest.raises(ValueError):
        target_notional("martingale", 1, 1, ctx())


def test_percent_starting_balance():
    # 50% of the configured starting balance ($10k) -> $5k
    assert target_notional("percent_starting_balance", 0.5, 1.0, ctx()) == pytest.approx(5_000.0)
