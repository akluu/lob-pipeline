# lob-pipeline

Spark pipeline that turns CME market-by-order data (Databento MBO) into limit order
book files in the [LOBSTER](https://lobsterdata.com) format.

Built on two months of E-mini S&P 500 futures (ES), September and October 2025,
about 486 million order messages.

## How it works

1. `ingest` converts Databento `.dbn.zst` files to Parquet, one partition per UTC day.
2. `spark_job` rebuilds the order book for every instrument-day in parallel. Each MBO
   record becomes zero, one or two LOBSTER events, and each event is written with the
   top 10 price levels after it, between 09:30 and 16:00 New York time.
3. `export` writes the LOBSTER message and orderbook CSV files for the requested days.
4. `check` reads a CSV pair back and confirms every orderbook row follows from its
   message. It takes the opening book and anything below level 10 as given.

Every day in these files starts with a book snapshot (Databento's at 00:00 UTC, or CME's
at the Sunday reopen), so days are independent and can be rebuilt in parallel. The
rebuild refuses a day that does not start with one.

## Usage

Needs Python 3.12+ and Java 17 or 21 for Spark (tested with Java 21).

```
pip install -r requirements.txt
python -m lobpipe.ingest --out data/mbo es-mbo-2025-09.dbn.zst es-mbo-2025-10.dbn.zst
python -m lobpipe.spark_job --src data/mbo --out data/lob --verify
python -m lobpipe.export --src data/lob --out data/lobster --dates 2025-10-01 2025-10-02
python -m lobpipe.check data/lobster/*message_10.csv
```

`--verify` rebuilds the book a second way from adds, cancels and modifies only and
stops if the two ever disagree. Market data is not included in this repo.

## Event mapping

| MBO record | LOBSTER event |
|---|---|
| add | 1 new order |
| cancel | 3 delete |
| modify to a smaller size | 2 partial cancel (3 if the size goes to zero) |
| modify to a new price or a larger size | 3 delete, then 1 new order |
| modify that restores an iceberg's displayed size after a fill | 1 new order |
| fill of a resting order | 4 execution |
| fill beyond an order's displayed size (iceberg reserve) | 5 hidden execution |
| fill against an aggressor's remainder that was never shown in the book | 5 hidden execution |
| cancel or modify that only confirms a fill already written | none |
| fill of the aggressing order, trade summary, snapshot record | none |

Fills of the aggressing order are skipped because LOBSTER records executions on the
resting side. The rebuild raises an error on any sequence it can't explain instead of
guessing.

## Validation

- Every fill has to add up to its trade, and every cancel or modify has to match the
  order it refers to. All 486 million messages pass.
- On the days checked, the rebuilt book at the end of the day matches Databento's
  next-day snapshot order for order.
- `check` replays the exported CSV files on their own, with no access to the source data.
- The files load in [LOBFrame](https://github.com/FinancialComputingUCL/LOBFrame)
  (commit `c47549d`), the code behind *Deep Limit Order Book Forecasting* (Briola,
  Bartolucci, Aste), without changes. Its data processing and labels run on ten days of
  output, and DeepLOB trained for one epoch on CPU. Its evaluation stage was not run to
  completion.

## Tests

```
pytest
```
