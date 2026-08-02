from polyflux import Trade, Transfer, Resolution, MoneyFlow


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


# --- Transfer (deposit / p2p_transfer) -----------------------------------

def test_transfer_p2p_from_dict():
    t = Transfer.from_dict({
        "event_type": "p2p_transfer",
        "from_address": "0xce25e214d5cfe4f459cf67f08df581885aae7fdc",
        "to_address": "0x6566f92ad1b3baba23a65f1d49f57b3d52de7390",
        "amount": 3.8556, "token": "pUSD", "tier": "confirmed",
        "pm_link_reason": "pusd_user2user",
        "block_number": 91204599, "tx_hash": "0x9855",
        "timestamp": "2026-07-31T15:31:57.935445+00:00",
    })
    assert t.is_p2p and not t.is_deposit
    assert t.is_confirmed
    assert t.amount == 3.8556 and t.token == "pUSD"
    assert t.time is not None and t.time.year == 2026


def test_transfer_deposit_flag():
    t = Transfer.from_dict({"event_type": "deposit", "amount": "100", "token": "USDC.e"})
    assert t.is_deposit and not t.is_p2p
    assert t.amount == 100.0


# --- Resolution (propose / dispute / settle) -----------------------------

def test_resolution_propose_from_dict():
    r = Resolution.from_dict({
        "event_type": "propose",
        "oracle_contract": "0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1",
        "proposer": "0x1fd9885227d84e387d5aae46f187b9ff3a4d0ec8",
        "market_id": "3060332", "ancillary_hash": "abc123",
        "proposed_price": 1.0, "outcome": "YES",
        "oracle_timestamp": 1784823436,
        "ancillary_text": "q: title: Team A vs Team B, description: In the game...",
        "timestamp": "2026-07-31T15:32:14+00:00",
    })
    assert r.is_propose and not r.is_dispute
    assert r.outcome == "YES"
    assert r.market_key == "abc123"          # hash preferred
    assert r.title == "Team A vs Team B"
    assert r.time is not None


def test_resolution_market_key_falls_back_to_market_id():
    r = Resolution.from_dict({"event_type": "settle", "market_id": "42",
                              "ancillary_hash": None, "resolved_price": 0.0,
                              "outcome": "NO"})
    assert r.is_settle
    assert r.market_key == "42"


# --- MoneyFlow (deposit / withdrawal x type) ------------------------------

def test_moneyflow_deposit_external():
    m = MoneyFlow.from_dict({"event_type": "money_flow", "op": "deposit",
        "flow_type": "external", "wallet_address": "0xabc", "counterparty": None,
        "amount": "350.0", "token": "pUSD", "direction": "in",
        "block_number": "91", "tx_hash": "0xdead"})
    assert m.is_deposit and not m.is_withdrawal
    assert m.is_external and not m.is_p2p and not m.is_unidentified
    assert m.type == "external" and m.amount == 350.0 and m.direction == "in"
    assert m.block_number == 91


def test_moneyflow_p2p_and_type_alias():
    # wire uses "flow_type"; a plain "type" is also tolerated
    m = MoneyFlow.from_dict({"op": "withdrawal", "type": "p2p", "amount": "9.97",
        "wallet_address": "0x1", "counterparty": "0x2", "token": "pUSD", "direction": "out"})
    assert m.is_withdrawal and m.is_p2p
    assert m.counterparty == "0x2" and m.type == "p2p"


def test_moneyflow_missing_fields_safe():
    m = MoneyFlow.from_dict({})
    assert m.op is None and m.amount is None and m.time is None
