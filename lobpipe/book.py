"""Order book rebuild for one instrument-day and translation to LOBSTER events.

Records are processed in file order and grouped into events (records up to and
including the next one with F_LAST set). A trade summary (T) opens a trade group made
of the T and the fills (F) right after it.

Two views of the book are kept:
  orders  - the exchange's own book, changed only by A/C/M/R records
  depth   - the published book: orders minus passive fills that the exchange has
            reported but not yet confirmed with the order's C/M record

At the end of every event both views must agree; with verify=True a second depth
table built only from A/C/M/R is kept and compared after every event. Any record
sequence not covered by the rules below raises BookError instead of being guessed at.

LOBSTER event types written:
  1 new order, 2 partial cancel, 3 delete, 4 visible execution,
  5 execution against quantity that was never displayed (depth unchanged): the part
    of a fill beyond an order's displayed size (iceberg reserve), or a fill against the
    remainder of an aggressor from earlier in the same event that was never added
Fills of the aggressing order itself are not written: LOBSTER records executions on
the resting side. A modify that changes price or increases size is written as a
delete followed by a new order with the same order id. Auction fills (side N) are
only allowed outside the export window and are not written.
"""

from bisect import bisect_left, insort

F_LAST = 128
F_SNAPSHOT = 32
PRICE_SCALE = 100_000  # Databento 1e-9 units -> LOBSTER dollars x 10000
EMPTY_ASK = 9_999_999_999
EMPTY_BID = -9_999_999_999
DIRECTION = {"B": 1, "A": -1}
OPPOSITE = {"B": "A", "A": "B"}

COLUMNS = ["row_nr", "sub", "ts_event", "time_ns", "type", "order_id", "size", "price", "direction"]


def book_columns(levels):
    cols = []
    for i in range(1, levels + 1):
        cols += [f"ask_price_{i}", f"ask_size_{i}", f"bid_price_{i}", f"bid_size_{i}"]
    return cols


class BookError(Exception):
    pass


class Book:
    def __init__(self, levels=10, window=(34_200 * 10**9, 57_600 * 10**9), verify=False):
        self.levels = levels
        self.verify = verify
        self.native = {"B": {}, "A": {}}  # depth from A/C/M/R only, kept when verify=True
        self.lo, self.hi = window
        self.orders = {}  # order_id -> [side, price, size]
        self.pending = {}  # order_id -> visible quantity filled, waiting for its C/M
        self.depth = {"B": {}, "A": {}}  # price -> displayed quantity
        self.prices = {"B": [], "A": []}  # sorted ascending
        self.initialized = False
        self.max_time = None  # latest time_ns seen outside a reset; never moves backwards
        self.unshown = {}  # aggressors earlier in this event that were never in the book -> (side, price)
        self.event = []  # records of the open event
        self.out = []
        self.stats = {}

    # ---- input -------------------------------------------------------------

    def feed(self, rows):
        """rows: iterable of tuples (row_nr, ts_event, time_ns, action, side, order_id, price, size, flags)."""
        for r in rows:
            self.event.append(r)
            if r[8] & F_LAST:
                self._run_event(self.event)
                self.event = []
        out, self.out = self.out, []
        return out

    def finish(self):
        if self.event:
            raise BookError(f"input ended inside an event starting at row {self.event[0][0]}")

    # ---- published depth ---------------------------------------------------

    def _change(self, side, price, qty):
        levels = self.depth[side]
        new = levels.get(price, 0) + qty
        if new < 0:
            raise BookError(f"negative depth {side} {price}")
        if new == 0:
            if price in levels:
                del levels[price]
                prices = self.prices[side]
                del prices[bisect_left(prices, price)]
        else:
            if price not in levels:
                insort(self.prices[side], price)
            levels[price] = new

    def top(self):
        asks, bids = self.prices["A"], self.prices["B"]
        ad, bd = self.depth["A"], self.depth["B"]
        row = []
        for i in range(self.levels):
            if i < len(asks):
                p = asks[i]
                row += [p // PRICE_SCALE, ad[p]]
            else:
                row += [EMPTY_ASK, 0]
            if i < len(bids):
                p = bids[-1 - i]
                row += [p // PRICE_SCALE, bd[p]]
            else:
                row += [EMPTY_BID, 0]
        return row

    def best(self):
        bid = self.prices["B"][-1] if self.prices["B"] else None
        ask = self.prices["A"][0] if self.prices["A"] else None
        return bid, ask

    def _emit(self, r, sub, kind, oid, size, price, side):
        if self.lo <= r[2] < self.hi:
            if price % PRICE_SCALE:
                raise BookError(f"row {r[0]}: price {price} is not a whole LOBSTER price")
            self.out.append((r[0], sub, r[1], r[2], kind, oid, size, price // PRICE_SCALE, DIRECTION[side])
                            + tuple(self.top()))

    def check(self):
        """Published depth must equal the order book once all fills are confirmed."""
        depth = {"B": {}, "A": {}}
        for side, price, size in self.orders.values():
            depth[side][price] = depth[side].get(price, 0) + size
        if self.pending or depth != self.depth or any(sorted(depth[s]) != self.prices[s] for s in depth):
            raise BookError("published depth is out of sync with the order book")

    def _native(self, side, price, qty):
        levels = self.native[side]
        new = levels.get(price, 0) + qty
        if new:
            levels[price] = new
        else:
            levels.pop(price, None)

    def _count(self, key):
        self.stats[key] = self.stats.get(key, 0) + 1

    # ---- events ------------------------------------------------------------

    def _reset(self):
        self.orders.clear()
        self.pending.clear()
        self.depth = {"B": {}, "A": {}}
        self.prices = {"B": [], "A": []}
        self.native = {"B": {}, "A": {}}

    def _snapshot(self, ev):
        if self.max_time is not None and self.lo <= self.max_time < self.hi:
            raise BookError(f"row {ev[0][0]}: book reset inside the export window")
        if self.pending:
            raise BookError(f"row {ev[0][0]}: book reset with unconfirmed fills")
        if ev[0][3] != "R" or any(r[3] != "A" or not r[8] & F_SNAPSHOT for r in ev[1:]):
            raise BookError(f"row {ev[0][0]}: malformed snapshot event")
        self._reset()
        self.initialized = False
        for r in ev[1:]:
            oid, side = r[5], r[4]
            if side not in DIRECTION or oid in self.orders:
                raise BookError(f"row {r[0]}: bad snapshot order {oid} side {side}")
            self.orders[oid] = [side, r[6], r[7]]
            self._change(side, r[6], r[7])
            if self.verify:
                self._native(side, r[6], r[7])
        self.initialized = True
        self._count("snapshot")

    def _run_event(self, ev):
        if any(r[3] == "R" for r in ev):
            self._snapshot(ev)
            return
        if not self.initialized:
            raise BookError(f"row {ev[0][0]}: event before the book was initialized")
        if any(r[8] & F_SNAPSHOT for r in ev):
            raise BookError(f"row {ev[0][0]}: snapshot record outside a reset")
        t = max(r[2] for r in ev)
        if self.max_time is None or t > self.max_time:
            self.max_time = t
        resting = {r[5] for r in ev if r[3] == "T" and r[5] in self.orders}
        self.unshown = {}
        group = None
        for r in ev:
            act = r[3]
            if act != "F" and group is not None:
                self._close_group(group, resting)
                group = None
            if act in ("A", "C", "M"):
                self.unshown.pop(r[5], None)
            if act == "A":
                self._add(r)
            elif act == "C":
                self._cancel(r)
            elif act == "M":
                self._modify(r)
            elif act == "T":
                group = self._trade(r)
                if r[4] in DIRECTION and r[5] not in self.orders:
                    self.unshown[r[5]] = (r[4], r[6])
            elif act == "F":
                if group is None:
                    raise BookError(f"row {r[0]}: fill without a trade summary")
                self._fill(r, group)
            elif act != "N":
                raise BookError(f"row {r[0]}: unknown action {act}")
        if group is not None:
            self._close_group(group, resting)
        if self.pending:
            raise BookError(f"row {ev[-1][0]}: fills not confirmed by the end of the event: {self.pending}")
        if self.verify and self.native != self.depth:
            raise BookError(f"row {ev[-1][0]}: published depth differs from the order book after the event")

    def _add(self, r):
        oid, side, price, size = r[5], r[4], r[6], r[7]
        if side not in DIRECTION:
            raise BookError(f"row {r[0]}: add with side {side}")
        if oid in self.orders:
            raise BookError(f"row {r[0]}: add for live order {oid}")
        self.orders[oid] = [side, price, size]
        self._change(side, price, size)
        if self.verify:
            self._native(side, price, size)
        self._emit(r, 0, 1, oid, size, price, side)
        self._count("add")

    def _trade(self, r):
        if r[4] == "N":
            if self.lo <= r[2] < self.hi:
                raise BookError(f"row {r[0]}: auction trade inside the export window")
            bid, ask = self.best()
            if bid is None or ask is None or bid < ask:
                raise BookError(f"row {r[0]}: auction trade on an uncrossed book")
            self._count("auction")
        return {"t": r, "passive": 0, "aggressor": 0, "fills": 0}

    def _fill(self, r, g):
        t = g["t"]
        oid, side, price, qty = r[5], r[4], r[6], r[7]
        g["fills"] += qty
        if t[4] == "N":
            if price != t[6]:
                raise BookError(f"row {r[0]}: auction fill off the auction price")
            if oid not in self.orders:
                if side != "N":
                    raise BookError(f"row {r[0]}: auction fill for unknown order {oid}")
                return
            o = self.orders[oid]
            if side not in ("N", o[0]):
                raise BookError(f"row {r[0]}: auction fill on the wrong side")
            visible = min(qty, o[2] - self.pending.get(oid, 0))
            self._pend(oid, o, visible)
            return
        if oid == t[5]:
            if side != t[4] or price != t[6]:
                raise BookError(f"row {r[0]}: aggressor fill does not match its trade")
            g["aggressor"] += qty
            return
        o = self.orders.get(oid)
        if o is None:
            if self.unshown.get(oid) == (side, price) and side == OPPOSITE[t[4]] and price == t[6]:
                g["passive"] += qty
                self._emit(r, 0, 5, oid, qty, price, side)
                self._count("unshown_remainder_execution")
                return
            raise BookError(f"row {r[0]}: fill for unknown order {oid}")
        if side != OPPOSITE[t[4]] or o[0] != side:
            raise BookError(f"row {r[0]}: passive fill on the wrong side")
        if price != t[6] or o[1] != price:
            raise BookError(f"row {r[0]}: passive fill price {price} vs trade {t[6]} / order {o[1]}")
        g["passive"] += qty
        visible = min(qty, o[2] - self.pending.get(oid, 0))
        hidden = qty - visible
        self._pend(oid, o, visible)
        if visible:
            self._emit(r, 0, 4, oid, visible, price, side)
            self._count("execution")
        if hidden:
            self._emit(r, 1, 5, oid, hidden, price, side)
            self._count("hidden_execution")

    def _pend(self, oid, o, visible):
        if visible:
            self.pending[oid] = self.pending.get(oid, 0) + visible
            self._change(o[0], o[1], -visible)

    def _close_group(self, g, resting):
        t = g["t"]
        if t[4] == "N":
            if g["fills"] != 2 * t[7]:
                raise BookError(f"row {t[0]}: auction fills {g['fills']} for trade size {t[7]}")
            return
        if g["passive"] != t[7]:
            raise BookError(f"row {t[0]}: passive fills {g['passive']} for trade size {t[7]}")
        want = t[7] if t[5] in resting else 0
        if g["aggressor"] != want:
            raise BookError(f"row {t[0]}: aggressor fills {g['aggressor']}, expected {want}")

    def _cancel(self, r):
        oid = r[5]
        o = self.orders.get(oid)
        if o is None:
            raise BookError(f"row {r[0]}: cancel for unknown order {oid}")
        if r[7] != o[2]:
            raise BookError(f"row {r[0]}: cancel size {r[7]} vs order size {o[2]}")
        filled = self.pending.pop(oid, 0)
        rest = o[2] - filled
        del self.orders[oid]
        if self.verify:
            self._native(o[0], o[1], -o[2])
        if rest:
            self._change(o[0], o[1], -rest)
            self._emit(r, 0, 3, oid, rest, o[1], o[0])
            self._count("delete")
        else:
            self._count("fill_done")

    def _modify(self, r):
        oid, price, new = r[5], r[6], r[7]
        o = self.orders.get(oid)
        if o is None:
            raise BookError(f"row {r[0]}: modify for unknown order {oid}")
        side, old_price, size = o
        if r[4] != side:
            raise BookError(f"row {r[0]}: modify changes side")
        filled = self.pending.pop(oid, 0)
        rest = size - filled
        if self.verify:
            self._native(side, old_price, -size)
            self._native(side, price, new)
        if price != old_price:
            if rest:
                self._change(side, old_price, -rest)
                self._emit(r, 0, 3, oid, rest, old_price, side)
            if new:
                self._change(side, price, new)
                self._emit(r, 1, 1, oid, new, price, side)
            self._count("replace")
        elif new < rest:
            self._change(side, price, new - rest)
            self._emit(r, 0, 2 if new else 3, oid, rest - new, price, side)
            self._count("partial_cancel" if new else "delete")
        elif new > rest:
            if filled and rest:
                raise BookError(f"row {r[0]}: size restored on a partly filled order")
            if rest:
                self._change(side, price, -rest)
                self._emit(r, 0, 3, oid, rest, price, side)
            self._change(side, price, new)
            self._emit(r, 1, 1, oid, new, price, side)
            self._count("replenish" if filled else "increase")
        if new:
            o[1], o[2] = price, new
        else:
            del self.orders[oid]
