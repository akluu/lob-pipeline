import random

from lobpipe.book import F_LAST, F_SNAPSHOT

IN = 40_000 * 10**9  # inside the default 09:30-16:00 window
OUT = 30_000 * 10**9  # before it


def px(x):
    return round(x * 1e9)


class Stream:
    """Builds feed() rows: (row_nr, ts_event, time_ns, action, side, order_id, price, size, flags)."""

    def __init__(self, t=IN):
        self.rows = []
        self.t = t

    def event(self, *recs, t=None):
        t = self.t if t is None else t
        for i, (act, side, oid, price, size) in enumerate(recs):
            flags = F_LAST if i == len(recs) - 1 else 0
            self.rows.append((len(self.rows), t, t, act, side, oid, px(price), size, flags))
        return self

    def snapshot(self, *orders, t=OUT):
        self.rows.append((len(self.rows), t, t, "R", "N", 0, 2**63 - 1, 0, F_SNAPSHOT))
        for i, (side, oid, price, size) in enumerate(orders):
            flags = F_SNAPSHOT | (F_LAST if i == len(orders) - 1 else 0)
            self.rows.append((len(self.rows), t, t, "A", side, oid, px(price), size, flags))
        return self


def base():
    return Stream().snapshot(("B", 1, 100.00, 5), ("B", 2, 99.75, 3), ("A", 3, 100.25, 4), ("A", 4, 100.50, 2))


def random_stream(seed, n_events=400):
    rng = random.Random(seed)
    s = Stream().snapshot(*[("B", i, 100.0 - 0.25 * (i % 6), 1 + i % 3) for i in range(1, 13)],
                          *[("A", i, 100.25 + 0.25 * (i % 6), 1 + i % 3) for i in range(13, 25)])
    live = {i: ("B" if i < 13 else "A", 100.0 - 0.25 * (i % 6) if i < 13 else 100.25 + 0.25 * (i % 6), 1 + i % 3)
            for i in range(1, 25)}
    nxt = 100
    for _ in range(n_events):
        kind = rng.random()
        if kind < 0.4 or len(live) < 6:
            side = rng.choice("BA")
            price = 100.0 - 0.25 * rng.randrange(6) if side == "B" else 100.25 + 0.25 * rng.randrange(6)
            size = rng.randrange(1, 5)
            s.event(("A", side, nxt, price, size))
            live[nxt] = (side, price, size)
            nxt += 1
        elif kind < 0.7:
            oid = rng.choice(sorted(live))
            side, price, size = live.pop(oid)
            s.event(("C", side, oid, price, size))
        else:
            oid = rng.choice(sorted(live))
            side, price, size = live[oid]
            new = max(1, size + rng.choice([-1, 1]))
            s.event(("M", side, oid, price, new))
            live[oid] = (side, price, new)
    return s
