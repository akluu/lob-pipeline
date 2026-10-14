"""Order book for one instrument-day, written out as LOBSTER events.

Records are grouped into events (up to and including the next record with F_LAST). Two
views of the book are kept: `orders`, the exchange's book changed only by add, cancel,
modify and reset records, and `depth`, the published book, which also takes off fills as
soon as they happen. Both must agree at the end of every event. Anything the rules below
don't explain raises BookError instead of being guessed at.

A record is a tuple (row_nr, ts_event, time_ns, action, side, order_id, price, size, flags).
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
    return [f"{name}_{i}" for i in range(1, levels + 1) for name in ("ask_price", "ask_size", "bid_price", "bid_size")]


class BookError(Exception):
    pass


class Book:
    def __init__(self, levels=10, window=(34_200 * 10**9, 57_600 * 10**9), verify=False):
        self.levels = levels
        self.lo, self.hi = window  # only events in [lo, hi) nanoseconds after midnight are written
        self.verify = verify  # also keep depth built from add/cancel/modify only, and compare
        self.orders = {}  # order_id -> [side, price, size]
        self.pending = {}  # order_id -> filled quantity not yet confirmed by its cancel/modify
        self.depth = {"B": {}, "A": {}}  # price -> displayed size
        self.prices = {"B": [], "A": []}  # sorted ascending
        self.native = {"B": {}, "A": {}}
        self.initialized = False
        self.max_time = None
        self.unshown = {}  # aggressors earlier in this event that never rested -> (side, price)
        self.event = []
        self.out = []

    def feed(self, records):
        for rec in records:
            self.event.append(rec)
            if rec[8] & F_LAST:
                self._run_event(self.event)
                self.event = []
        out, self.out = self.out, []
        return out

    def finish(self):
        if self.event:
            raise BookError(f"input ended inside an event starting at row {self.event[0][0]}")

    def top(self):
        asks, bids = self.prices["A"], self.prices["B"]
        row = []
        for i in range(self.levels):
            if i < len(asks):
                row += [asks[i] // PRICE_SCALE, self.depth["A"][asks[i]]]
            else:
                row += [EMPTY_ASK, 0]
            if i < len(bids):
                price = bids[-1 - i]
                row += [price // PRICE_SCALE, self.depth["B"][price]]
            else:
                row += [EMPTY_BID, 0]
        return row

    def _change(self, side, price, qty):
        if price <= 0 or price % PRICE_SCALE:
            raise BookError(f"price {price} is not a positive whole LOBSTER price")  # e.g. a calendar spread
        levels, size = self.depth[side], self.depth[side].get(price, 0) + qty
        if size < 0:
            raise BookError(f"negative depth {side} {price}")
        if size == 0 and price in levels:
            del levels[price]
            del self.prices[side][bisect_left(self.prices[side], price)]
        elif size:
            if price not in levels:
                insort(self.prices[side], price)
            levels[price] = size

    def _native(self, side, price, qty):
        size = self.native[side].get(price, 0) + qty
        if size:
            self.native[side][price] = size
        else:
            self.native[side].pop(price, None)

    def _emit(self, rec, sub, kind, oid, size, price, side):
        if self.lo <= rec[2] < self.hi:
            if price % PRICE_SCALE:
                raise BookError(f"row {rec[0]}: price {price} is not a whole LOBSTER price")
            self.out.append((rec[0], sub, rec[1], rec[2], kind, oid, size, price // PRICE_SCALE, DIRECTION[side])
                            + tuple(self.top()))

    def _snapshot(self, event):
        """A reset (R) followed by snapshot adds; files from before February 2024 have no R."""
        first = event[0][0]
        if self.max_time is not None and self.lo <= self.max_time < self.hi:
            raise BookError(f"row {first}: book reset inside the export window")
        if self.pending:
            raise BookError(f"row {first}: book reset with unconfirmed fills")
        adds = event[1:] if event[0][3] == "R" else event
        if any(rec[3] != "A" or not rec[8] & F_SNAPSHOT for rec in adds):
            raise BookError(f"row {first}: malformed snapshot event")
        self.orders.clear()
        self.pending.clear()
        self.depth, self.prices, self.native = {"B": {}, "A": {}}, {"B": [], "A": []}, {"B": {}, "A": {}}
        self.initialized = False
        for rec in adds:
            row, side, oid, price, size = rec[0], rec[4], rec[5], rec[6], rec[7]
            if side not in DIRECTION or oid in self.orders:
                raise BookError(f"row {row}: bad snapshot order {oid} side {side}")
            self.orders[oid] = [side, price, size]
            self._change(side, price, size)
            if self.verify:
                self._native(side, price, size)
        self.initialized = True

    def _run_event(self, event):
        first, last = event[0][0], event[-1][0]
        if any(rec[3] == "R" for rec in event) or (not self.initialized and all(rec[8] & F_SNAPSHOT for rec in event)):
            self._snapshot(event)
            return
        if not self.initialized:
            raise BookError(f"row {first}: event before the book was initialized")
        if any(rec[8] & F_SNAPSHOT for rec in event):
            raise BookError(f"row {first}: snapshot record outside a reset")
        latest = max(rec[2] for rec in event)
        self.max_time = latest if self.max_time is None else max(self.max_time, latest)
        resting = {rec[5] for rec in event if rec[3] == "T" and rec[5] in self.orders}
        self.unshown = {}
        group = None
        for rec in event:
            action = rec[3]
            if action != "F" and group is not None:
                self._close_group(group, resting)
                group = None
            if action in ("A", "C", "M"):
                self.unshown.pop(rec[5], None)
            if action == "A":
                self._add(rec)
            elif action == "C":
                self._cancel(rec)
            elif action == "M":
                self._modify(rec)
            elif action == "T":
                group = self._trade(rec)
                if rec[4] in DIRECTION and rec[5] not in self.orders:
                    self.unshown[rec[5]] = (rec[4], rec[6])
            elif action == "F":
                if group is None:
                    raise BookError(f"row {rec[0]}: fill without a trade summary")
                self._fill(rec, group)
            elif action != "N":
                raise BookError(f"row {rec[0]}: unknown action {action}")
        if group is not None:
            self._close_group(group, resting)
        if self.pending:
            raise BookError(f"row {last}: fills not confirmed by the end of the event: {self.pending}")
        if self.verify and self.native != self.depth:
            raise BookError(f"row {last}: published depth differs from the order book after the event")

    def _add(self, rec):
        row, side, oid, price, size = rec[0], rec[4], rec[5], rec[6], rec[7]
        if side not in DIRECTION:
            raise BookError(f"row {row}: add with side {side}")
        if oid in self.orders:
            raise BookError(f"row {row}: add for live order {oid}")
        self.orders[oid] = [side, price, size]
        self._change(side, price, size)
        if self.verify:
            self._native(side, price, size)
        self._emit(rec, 0, 1, oid, size, price, side)

    def _trade(self, rec):
        if rec[4] == "N":  # opening auction: only outside the window, on a crossed book
            bid = self.prices["B"][-1] if self.prices["B"] else None
            ask = self.prices["A"][0] if self.prices["A"] else None
            if self.lo <= rec[2] < self.hi:
                raise BookError(f"row {rec[0]}: auction trade inside the export window")
            if bid is None or ask is None or bid < ask:
                raise BookError(f"row {rec[0]}: auction trade on an uncrossed book")
        return {"trade": rec, "passive": 0, "aggressor": 0, "fills": 0}

    def _fill(self, rec, group):
        trade = group["trade"]
        row, side, oid, price, qty = rec[0], rec[4], rec[5], rec[6], rec[7]
        group["fills"] += qty
        if trade[4] == "N":
            if price != trade[6]:
                raise BookError(f"row {row}: auction fill off the auction price")
            if oid not in self.orders:
                if side != "N":
                    raise BookError(f"row {row}: auction fill for unknown order {oid}")
                return
            order = self.orders[oid]
            if side not in ("N", order[0]):
                raise BookError(f"row {row}: auction fill on the wrong side")
            self._pend(oid, order, min(qty, order[2] - self.pending.get(oid, 0)))
            return
        if oid == trade[5]:
            if side != trade[4] or price != trade[6]:
                raise BookError(f"row {row}: aggressor fill does not match its trade")
            group["aggressor"] += qty
            return
        order = self.orders.get(oid)
        if order is None:
            # fill against an earlier aggressor's remainder that was never shown in the book
            if self.unshown.get(oid) == (side, price) and side == OPPOSITE[trade[4]] and price == trade[6]:
                group["passive"] += qty
                self._emit(rec, 0, 5, oid, qty, price, side)
                return
            raise BookError(f"row {row}: fill for unknown order {oid}")
        if side != OPPOSITE[trade[4]] or order[0] != side:
            raise BookError(f"row {row}: passive fill on the wrong side")
        if price != trade[6] or order[1] != price:
            raise BookError(f"row {row}: passive fill price {price} vs trade {trade[6]} / order {order[1]}")
        group["passive"] += qty
        visible = min(qty, order[2] - self.pending.get(oid, 0))
        self._pend(oid, order, visible)
        if visible:
            self._emit(rec, 0, 4, oid, visible, price, side)
        if qty > visible:  # iceberg reserve
            self._emit(rec, 1, 5, oid, qty - visible, price, side)

    def _pend(self, oid, order, visible):
        if visible:
            self.pending[oid] = self.pending.get(oid, 0) + visible
            self._change(order[0], order[1], -visible)

    def _close_group(self, group, resting):
        trade = group["trade"]
        row, size = trade[0], trade[7]
        if trade[4] == "N":
            if group["fills"] != 2 * size:
                raise BookError(f"row {row}: auction fills {group['fills']} for trade size {size}")
            return
        if group["passive"] != size:
            raise BookError(f"row {row}: passive fills {group['passive']} for trade size {size}")
        want = size if trade[5] in resting else 0
        if group["aggressor"] != want:
            raise BookError(f"row {row}: aggressor fills {group['aggressor']}, expected {want}")

    def _cancel(self, rec):
        row, oid = rec[0], rec[5]
        order = self.orders.get(oid)
        if order is None:
            raise BookError(f"row {row}: cancel for unknown order {oid}")
        side, price, size = order
        if rec[7] != size:
            raise BookError(f"row {row}: cancel size {rec[7]} vs order size {size}")
        rest = size - self.pending.pop(oid, 0)
        del self.orders[oid]
        if self.verify:
            self._native(side, price, -size)
        if rest:
            self._change(side, price, -rest)
            self._emit(rec, 0, 3, oid, rest, price, side)

    def _modify(self, rec):
        row, oid, price, size = rec[0], rec[5], rec[6], rec[7]
        order = self.orders.get(oid)
        if order is None:
            raise BookError(f"row {row}: modify for unknown order {oid}")
        side, old_price, old_size = order
        if rec[4] != side:
            raise BookError(f"row {row}: modify changes side")
        filled = self.pending.pop(oid, 0)
        rest = old_size - filled
        if self.verify:
            self._native(side, old_price, -old_size)
            self._native(side, price, size)
        if price != old_price:
            if rest:
                self._change(side, old_price, -rest)
                self._emit(rec, 0, 3, oid, rest, old_price, side)
            if size:
                self._change(side, price, size)
                self._emit(rec, 1, 1, oid, size, price, side)
        elif size < rest:
            self._change(side, price, size - rest)
            self._emit(rec, 0, 2 if size else 3, oid, rest - size, price, side)
        elif size > rest:  # a larger size, or an iceberg showing more after a fill
            if filled and rest:
                raise BookError(f"row {row}: size restored on a partly filled order")
            if rest:
                self._change(side, price, -rest)
                self._emit(rec, 0, 3, oid, rest, price, side)
            self._change(side, price, size)
            self._emit(rec, 1, 1, oid, size, price, side)
        if size:
            order[1], order[2] = price, size
        else:
            del self.orders[oid]
