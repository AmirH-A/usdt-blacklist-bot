"""Transition logic for the provider watchdog. Run: python3 test_provider_watch.py"""

from src.provider_watch import _decide, _down_message, _in_use, _label, _meaning

REMIND = 6 * 3600.0


def test_first_failure_alerts_once():
    state = {}
    action, state = _decide(state, ok=False, now=1000.0, remind_s=REMIND)
    assert action == "down", action
    # Still dead a minute later: silence, not a second alert.
    action, state = _decide(state, ok=False, now=1060.0, remind_s=REMIND)
    assert action is None, action


def test_reminder_after_window():
    _, state = _decide({}, ok=False, now=0.0, remind_s=REMIND)
    action, state = _decide(state, ok=False, now=REMIND - 1, remind_s=REMIND)
    assert action is None, action
    action, state = _decide(state, ok=False, now=REMIND, remind_s=REMIND)
    assert action == "remind", action
    # The reminder resets the clock, so it does not repeat every cycle.
    action, _ = _decide(state, ok=False, now=REMIND + 1, remind_s=REMIND)
    assert action is None, action


def test_recovery_alerts_then_goes_quiet():
    _, state = _decide({}, ok=False, now=0.0, remind_s=REMIND)
    action, state = _decide(state, ok=True, now=10.0, remind_s=REMIND)
    assert action == "up", action
    action, _ = _decide(state, ok=True, now=20.0, remind_s=REMIND)
    assert action is None, action


def test_healthy_endpoint_never_alerts():
    state = {}
    for t in range(5):
        action, state = _decide(state, ok=True, now=float(t), remind_s=REMIND)
        assert action is None, action


def test_message_tells_you_to_rotate_only_for_auth_codes():
    auth = _down_message("TRON", "node #1", 403, "CHAINSTACK_API_TOKEN", 1, 2)
    assert "CHANGE API KEY" in auth
    assert "CHAINSTACK_API_TOKEN" in auth
    assert "quota" in auth

    # A network blip is not a key problem: no rotate instructions.
    blip = _down_message("ETH", "...abc123", 0, "ETH_RPC_URL", 0, 2)
    assert "CHANGE API KEY" not in blip
    assert "Chainstack" not in blip


def test_only_the_endpoint_in_use_is_reported():
    # Fallback (#2) dead, detection on #1: nothing to say about #2.
    assert _in_use([None, 403], 0) == [(0, None)]
    # Detection failed over to #2 and #2 is dead too: report #2 only.
    assert _in_use([403, 403], 1) == [(1, 403)]
    # Unknown active index: fall back to reporting every endpoint.
    assert _in_use([403, None], 5) == [(0, 403), (1, None)]


def test_label_hides_most_of_the_key():
    assert _label("https://host/6d4d4ea357f2fbb77d7a843a80f2ee5c") == "...f2ee5c"
    assert _meaning(401).startswith("key not recognised")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all passed")
