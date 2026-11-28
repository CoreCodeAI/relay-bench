import json
import builtins
import types
import importlib

# Import the module under test
relay = importlib.import_module("relay_bench")

def test_official_cost_basic():
    # Prepare a mock usage dict with known token counts
    usage = {
        "input_tokens": 1000,
        "output_tokens": 2000,
        "cache_creation": {
            "ephemeral_5m_input_tokens": 300,
            "ephemeral_1h_input_tokens": 100,
        },
        "cache_read_input_tokens": 50,
    }
    # Set a known price per million tokens
    relay.CFG = {"price_per_mtok": {"input": 4.0, "output": 20.0,
                                   "cache_write_5m": 5.0, "cache_write_1h": 8.0,
                                   "cache_read": 0.20}}
    # Expected cost calculation:
    # input: 1000 * 4 = 4000
    # output: 2000 * 20 = 40000
    # cache_write_5m: 300 * 5 = 1500
    # cache_write_1h: 100 * 8 = 800
    # cache_read: 50 * 0.20 = 10
    # Sum = 46310, then divide by 1e6 => 0.04631
    expected = (1000 * 4.0 + 2000 * 20.0 + 300 * 5.0 + 100 * 8.0 + 50 * 0.20) / 1e6
    assert abs(relay.official_cost(usage) - expected) < 1e-9

def test_u_ctx_counts():
    # Mock usage dict with various token fields
    usage = {
        "input_tokens": 120,
        "cache_creation_input_tokens": 30,
        "cache_read_input_tokens": 10,
    }
    # u_ctx should sum these three fields
    assert relay.u_ctx(usage) == 120 + 30 + 10

def test_balance_path_none_returns_none(monkeypatch):
    # Create a dummy site object with no balance_path
    class DummySite:
        balance_path = ""
        balance_divisor = 100
        base = "http://example.com"
        key = "dummy"
    dummy = DummySite()
    # balance should return None when balance_path is falsy
    assert relay.balance(dummy) is None

def test_balance_success(monkeypatch):
    # Mock http_json to return a successful response
    def fake_http_json(site, method, path, timeout=30):
        return 200, {"total_usage": 12345}, {}
    monkeypatch.setattr(relay, "http_json", fake_http_json)

    class DummySite:
        balance_path = "/v1/dashboard/billing/usage"
        balance_divisor = 100
        base = "http://example.com"
        key = "dummy"
    dummy = DummySite()
    # Expected usage = 12345 / 100 = 123.45
    assert relay.balance(dummy) == 123.45
