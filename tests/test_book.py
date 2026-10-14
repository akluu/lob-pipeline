import pytest

from lobpipe.book import EMPTY_ASK, EMPTY_BID, F_LAST, F_SNAPSHOT, Book, BookError
from tests.streams import IN, OUT, Stream, base, px, random_stream

def consistent(b):
    """Published depth equals the order book once every fill is confirmed."""
    depth = {"B": {}, "A": {}}
    for side, price, size in b.orders.values():
        depth[side][price] = depth[side].get(price, 0) + size
    assert not b.pending and depth == b.depth
    assert all(sorted(depth[s]) == b.prices[s] for s in depth)


def run(stream, levels=2):
    b = Book(levels=levels, verify=True)
    out = b.feed(stream.rows)
    b.finish()
    return b, out


def msgs(out):
    return [(m[4], m[5], m[6], m[7], m[8]) for m in out]


def lob(x):
    return round(x * 10_000)


def test_add_cancel_reduce_replace_increase():
    s = base().event(("A", "B", 10, 100.00, 2))
    s.event(("M", "B", 10, 100.00, 1))  # reduce
    s.event(("M", "B", 10, 99.75, 1))  # reprice
    s.event(("M", "B", 10, 99.75, 4))  # increase
    s.event(("M", "B", 10, 99.75, 4))  # no change
    s.event(("C", "B", 10, 99.75, 4))
    b, out = run(s)
    assert msgs(out) == [
        (1, 10, 2, lob(100.00), 1),
        (2, 10, 1, lob(100.00), 1),
        (3, 10, 1, lob(100.00), 1), (1, 10, 1, lob(99.75), 1),
        (3, 10, 1, lob(99.75), 1), (1, 10, 4, lob(99.75), 1),
        (3, 10, 4, lob(99.75), 1),
    ]
    consistent(b)
    # book after the first add: bid 100.00 x 7, 99.75 x 3
    assert out[0][9:] == (lob(100.25), 4, lob(100.00), 7, lob(100.50), 2, lob(99.75), 3)


def test_modify_to_zero_is_a_delete():
    b, out = run(base().event(("M", "A", 4, 100.50, 0)))
    assert msgs(out) == [(3, 4, 2, lob(100.50), -1)]
    assert 4 not in b.orders
    consistent(b)


def test_passive_fills_full_and_partial():
    # buyer lifts 4 at 100.25 (order 3 fully), then 1 of order 4's 2 at 100.50
    s = base().event(("T", "B", 50, 100.25, 4), ("F", "A", 3, 100.25, 4), ("C", "A", 3, 100.25, 4))
    s.event(("T", "B", 51, 100.50, 1), ("F", "A", 4, 100.50, 1), ("M", "A", 4, 100.50, 1))
    b, out = run(s)
    assert msgs(out) == [(4, 3, 4, lob(100.25), -1), (4, 4, 1, lob(100.50), -1)]
    assert out[0][9:11] == (lob(100.50), 2)  # depth already reduced on the execution row
    assert out[1][9:11] == (lob(100.50), 1)
    consistent(b)


def test_iceberg_reserve_and_replenish():
    s = base().event(("A", "A", 20, 100.25, 1))
    s.event(("T", "B", 52, 100.25, 1), ("F", "A", 20, 100.25, 1), ("M", "A", 20, 100.25, 1))  # replenish
    s.event(("T", "B", 53, 100.25, 2), ("F", "A", 20, 100.25, 2), ("C", "A", 20, 100.25, 1))  # 1 shown, 1 reserve
    b, out = run(s)
    assert msgs(out) == [
        (1, 20, 1, lob(100.25), -1),
        (4, 20, 1, lob(100.25), -1), (1, 20, 1, lob(100.25), -1),
        (4, 20, 1, lob(100.25), -1), (5, 20, 1, lob(100.25), -1),
    ]
    assert out[-1][9:11] == out[-2][9:11]  # type 5 leaves depth unchanged
    consistent(b)


def test_fill_against_an_earlier_aggressor_that_was_never_shown():
    # sell 60 hits bid 1, then buy 61 (never in the book) lifts 60's remainder before it is added
    s = base().event(("T", "A", 60, 100.00, 1), ("F", "B", 1, 100.00, 1),
                     ("T", "B", 61, 100.00, 1), ("F", "A", 60, 100.00, 1),
                     ("M", "B", 1, 100.00, 4), ("A", "A", 60, 100.00, 2))
    b, out = run(s)
    assert msgs(out) == [(4, 1, 1, lob(100.00), 1), (5, 60, 1, lob(100.00), -1), (1, 60, 2, lob(100.00), -1)]
    assert out[1][9:13] == out[0][9:13]  # type 5 leaves the depth alone
    consistent(b)


@pytest.mark.parametrize("events", [
    # no earlier trade by order 60 in the event
    [[("T", "B", 61, 100.25, 1), ("F", "A", 60, 100.25, 1)]],
    # earlier aggressor, but the fill is on the wrong side
    [[("T", "A", 60, 100.00, 1), ("F", "B", 1, 100.00, 1), ("T", "A", 61, 100.00, 1), ("F", "B", 60, 100.00, 1),
      ("M", "B", 1, 100.00, 4)]],
    # earlier aggressor, but at another price
    [[("T", "A", 60, 100.00, 1), ("F", "B", 1, 100.00, 1), ("T", "B", 61, 100.25, 1), ("F", "A", 60, 100.25, 1),
      ("M", "B", 1, 100.00, 4)]],
    # the aggressor traded in an earlier event, not this one
    [[("T", "A", 60, 100.00, 1), ("F", "B", 1, 100.00, 1), ("M", "B", 1, 100.00, 4)],
     [("T", "B", 61, 100.00, 1), ("F", "A", 60, 100.00, 1)]],
])
def test_rejects_other_fills_on_unknown_orders(events):
    s = base()
    for ev in events:
        s.event(*ev)
    with pytest.raises(BookError, match="unknown order"):
        run(s)


def test_resting_order_becomes_aggressor():
    # order 4 (ask 100.50 x2) is repriced through the bid and trades 2 at 100.00 against order 1
    s = base().event(("T", "A", 4, 100.00, 2), ("F", "A", 4, 100.00, 2), ("F", "B", 1, 100.00, 2),
                     ("M", "B", 1, 100.00, 3), ("C", "A", 4, 100.50, 2))
    b, out = run(s)
    assert msgs(out) == [(4, 1, 2, lob(100.00), 1), (3, 4, 2, lob(100.50), -1)]
    consistent(b)


def test_out_of_window_records_update_book_but_are_not_written():
    s = base().event(("A", "B", 30, 99.50, 9), t=OUT).event(("A", "B", 31, 99.25, 1))
    b, out = run(s, levels=3)
    assert msgs(out) == [(1, 31, 1, lob(99.25), 1)]
    assert out[0][9 + 4 * 2 + 2:9 + 4 * 2 + 4] == (lob(99.50), 9)


def test_auction_outside_window():
    s = Stream().snapshot(("B", 1, 101.00, 2), ("A", 2, 100.00, 2), ("A", 3, 102.00, 1))
    s.event(("T", "N", 0, 100.50, 2), ("F", "N", 1, 100.50, 2), ("F", "N", 2, 100.50, 2),
            ("C", "B", 1, 101.00, 2), ("C", "A", 2, 100.00, 2), t=OUT)
    b, out = run(s)
    assert out == []
    consistent(b)
    assert b.prices["B"] == [] and b.prices["A"][0] == px(102.00)


def test_empty_levels_use_lobster_dummies():
    s = Stream().snapshot(("B", 1, 100.00, 1), t=OUT).event(("A", "A", 2, 100.25, 1))
    _, out = run(s, levels=2)
    assert out[0][9:] == (lob(100.25), 1, lob(100.00), 1, EMPTY_ASK, 0, EMPTY_BID, 0)


@pytest.mark.parametrize("extra, match", [
    (lambda s: s.event(("T", "B", 50, 100.25, 1), ("F", "A", 3, 100.25, 1), ("F", "A", 3, 100.25, 1),
                       ("M", "A", 3, 100.25, 2)), "passive fills"),
    (lambda s: s.event(("T", "B", 50, 100.25, 1), ("F", "A", 3, 100.25, 1)), "not confirmed"),
    (lambda s: s.event(("T", "B", 50, 100.25, 1), ("F", "B", 3, 100.25, 1), ("M", "A", 3, 100.25, 3)),
     "wrong side"),
    (lambda s: s.event(("C", "B", 99, 100.00, 1)), "unknown order"),
    (lambda s: s.event(("A", "B", 1, 100.00, 1)), "live order"),
    (lambda s: s.event(("T", "B", 50, 100.25, 2), ("F", "A", 3, 100.25, 2), ("M", "A", 3, 100.25, 3)),
     "restored"),
    (lambda s: s.event(("T", "N", 0, 100.10, 1), ("F", "N", 1, 100.10, 1), ("F", "N", 3, 100.10, 1),
                       ("M", "B", 1, 100.00, 4), ("M", "A", 3, 100.25, 3)), "auction trade inside"),
    (lambda s: s.event(("F", "A", 3, 100.25, 1)), "without a trade"),
])
def test_rejects_unexplained_sequences(extra, match):
    with pytest.raises(BookError, match=match):
        run(extra(base()))


def test_rejects_a_reset_inside_the_export_window():
    s = base().event(("A", "B", 10, 99.50, 1)).snapshot(("B", 1, 100.00, 5), t=IN)
    with pytest.raises(BookError, match="reset inside"):
        run(s)
    # an out-of-order earlier timestamp must not reopen the door
    back = base().event(("A", "B", 10, 99.50, 1)).event(("A", "B", 11, 99.25, 1), t=OUT)
    back.snapshot(("B", 1, 100.00, 5), t=IN)
    with pytest.raises(BookError, match="reset inside"):
        run(back)
    # a reset after the window closes does not touch exported rows
    late = base().event(("A", "B", 10, 99.50, 1)).event(("A", "B", 11, 99.25, 1), t=60_000 * 10**9)
    late.snapshot(("B", 1, 100.00, 5), t=60_000 * 10**9)
    run(late)


def test_rejects_events_before_or_outside_a_snapshot():
    with pytest.raises(BookError, match="initialized"):
        run(Stream().event(("A", "B", 1, 100.0, 1)))
    s = base()
    s.rows.append((len(s.rows), IN, IN, "A", "B", 77, px(99.0), 1, F_SNAPSHOT | F_LAST))
    with pytest.raises(BookError, match="snapshot record"):
        run(s)
    with pytest.raises(BookError, match="bad snapshot order"):
        run(Stream().snapshot(("B", 1, 100.0, 1), ("B", 1, 100.0, 2)))


def test_a_day_can_open_with_snapshot_adds_and_no_reset():
    with_reset = base().event(("A", "B", 10, 99.50, 1))
    without = base().event(("A", "B", 10, 99.50, 1))
    without.rows = without.rows[1:]  # files before February 2024 have no R record
    (b1, out1), (b2, out2) = run(without), run(with_reset)
    assert out1 == out2 and out1 and b1.orders == b2.orders
    # but only a block made entirely of snapshot adds counts, and only before anything else
    mixed = Stream()
    mixed.rows.append((0, OUT, OUT, "A", "B", 1, px(100.0), 5, F_SNAPSHOT))
    mixed.rows.append((1, OUT, OUT, "A", "B", 2, px(99.75), 5, F_LAST))
    with pytest.raises(BookError, match="initialized"):
        run(mixed)
    flagged_cancel = Stream()
    flagged_cancel.rows.append((0, OUT, OUT, "C", "B", 1, px(100.0), 5, F_SNAPSHOT | F_LAST))
    with pytest.raises(BookError, match="malformed snapshot"):
        run(flagged_cancel)


@pytest.mark.parametrize("price", [0.0, -1.25, 100.00001])
def test_rejects_prices_lobster_cannot_write(price):
    with pytest.raises(BookError, match="positive whole LOBSTER price"):
        run(base().event(("A", "B", 10, price, 1)))


def test_output_does_not_depend_on_batching():
    s = random_stream(7)
    whole = Book(levels=5, verify=True)
    expected = whole.feed(s.rows)
    consistent(whole)
    for cut in (1, 3, 17, 250):
        b = Book(levels=5)
        got = []
        for i in range(0, len(s.rows), cut):
            got += b.feed(s.rows[i:i + cut])
        b.finish()
        assert got == expected
