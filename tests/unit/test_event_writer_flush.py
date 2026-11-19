from featureforge.streaming.event_writer import idle_flush_due


def test_the_default_writes_a_partial_batch_at_the_first_idle_poll():
    assert idle_flush_due(100.0, 100.0, 0.0)


def test_a_partial_batch_waits_for_the_configured_time():
    assert not idle_flush_due(100.0, 129.9, 30.0)
    assert idle_flush_due(100.0, 130.0, 30.0)
