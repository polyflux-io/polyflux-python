from polyflux import Trade


def test_trade_from_dict_basic():
    t = Trade.from_dict({
        "event_type": "trade",
        "asset_id": "98022490269692409998126496127597032490334070080325855126491859374983463996227",
        "wallet_address": "0x1a8e478574d1f082a14465900300a3a2aef386f9",
        "size": "1.2987",
        "price": "0.48",
        "operation_type": "buy",
        "timestamp": 1784632933.412,
    })
    assert t.is_buy
    assert t.side == "buy"
    assert round(t.notional, 4) == round(1.2987 * 0.48, 4)
    assert t.time is not None and t.time.year >= 2026


def test_trade_tolerates_camelcase_and_ms():
    t = Trade.from_dict({"walletAddress": "0xabc", "operationType": "sell",
                         "size": 10, "price": 0.5, "timestamp": 1784632933412})
    assert t.wallet_address == "0xabc"
    assert t.side == "sell"
    assert t.time is not None  # ms epoch handled


def test_trade_missing_fields_safe():
    t = Trade.from_dict({})
    assert t.notional is None and t.time is None and t.side is None
