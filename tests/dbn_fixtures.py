import databento_dbn as dbn

ACTION = {"A": dbn.Action.ADD, "C": dbn.Action.CANCEL, "M": dbn.Action.MODIFY, "R": dbn.Action.CLEAR,
          "T": dbn.Action.TRADE, "F": dbn.Action.FILL, "N": dbn.Action.NONE}
SIDE = {"B": dbn.Side.BID, "A": dbn.Side.ASK, "N": dbn.Side.NONE}


def write_dbn(path, records, symbol="ESZ5", instrument_id=7):
    """records: dicts with ts_recv, ts_event, oid, px, sz, act, side, flags, seq, iid (optional)."""
    start = min(r["ts_recv"] for r in records)
    end = max(r["ts_recv"] for r in records) + 1
    md = dbn.Metadata(dataset="GLBX.MDP3", start=start, end=end, stype_in=dbn.SType.RAW_SYMBOL,
                      stype_out=dbn.SType.INSTRUMENT_ID, schema=dbn.Schema.MBO, symbols=[symbol], mappings=[])
    with open(path, "wb") as f:
        f.write(bytes(md.encode()))
        for r in records:
            msg = dbn.MBOMsg(1, r.get("iid", instrument_id), r["ts_event"], r["oid"], r["px"], r["sz"],
                             ACTION[r["act"]], SIDE[r["side"]], r["ts_recv"], flags=r["flags"], sequence=r["seq"])
            f.write(bytes(msg))
