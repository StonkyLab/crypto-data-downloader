# Transient network faults and Bybit relisting gaps — finding and fix

Date: 2026-09-06
Scope: `crypto-data-downloader` 2.7.7 (`8ab7395`), `bybit-cpp-api` `01e0b54`
Trigger: the Bybit spot run that finished on 2026-09-06 03:18 ended with a burst
of `boost::asio` error lines.

## Summary

The asio lines themselves were harmless: six `Connection reset by peer`
answers from Bybit during the 1m pass, printed verbosely by the new async
transport (2.7.6 prints the boost source location with every transport
error). Chasing where those six symbols' files were left, two real defects
came out, both older than this run and both responsible for silently
truncated 1m history:

1. **Transient network faults were never retried.** The per-symbol retry loops
   in the Bybit, Lighter and Hyperliquid downloaders retried only rate-limit
   rejections. A reset connection, a lost DNS answer or a timed-out socket
   ended the symbol on its first attempt — while the log said
   `attempt 1/5` — and left the CSV cut at that point.
2. **A Bybit symbol that was delisted and later relisted could never be
   downloaded past its first delisting.** The 1m history of SHIBUSDT froze at
   2021-11-29, LUNCUSDT and USTCUSDT at 2022-05-12, ZKJUSDT at 2024-06-10,
   GRAMUSDT at 2026-06-16, and the backup — written by the same code — was
   frozen the same way. These files were never complete.

Both are fixed. The retry fix covers the three venues that have a retry
loop; Binance, OKX and MEXC have no such loop and still end a symbol on
the first network fault (see "Not covered").

## Evidence

### The run

Log summary at 2026-09-06 03:18:57, `10 worker task(s) failed`:

| reason | symbols |
|---|---|
| `Transport failure … Connection reset by peer [system:104 …]` | MEWUSDT, PURSEUSDT, SPECUSDT, SUIUSDT, SWEATUSDT, TRXUSDT |
| `Bybit API error, code: 10001, msg: Not supported symbols` | BOBUSDT, SOSUSDT, TONUSDT, USTUSDT (delisted — expected) |

The six reset symbols' 1m files ended exactly at the `start=` timestamp of
the failed request; the 1h files of the still-trading ones were current.

### Defect 1 — retry loop

`src/bybit_downloader.cpp` (and the same shape in `lighter_downloader.cpp`,
`hyperliquid_downloader.cpp`, candles and funding alike):

```cpp
constexpr int maxRetries = 5;
for (int attempt = 0; attempt < maxRetries; ++attempt) {
    const auto resume = P::checkSymbolCSVFile(...);   // tail re-read every attempt
    try { ... download from resume ... return; }
    catch (const std::exception &e) {
        if (isRateLimitError(errMsg) && attempt < maxRetries - 1) { backoff; }
        else { throw runtime_error("... failed (attempt {}/{}) ..."); }   // <- every other error
    }
}
```

Only `10006` / `too many` / `429` were retried. Every transport error threw
on attempt 1. The loop is otherwise built for retrying — it re-reads the
tail before each attempt — so a retry is safe and resumes exactly where the
fault cut in.

The same pattern had already cost data elsewhere in the same week:

| run | venue | faults | effect |
|---|---|---|---|
| 2026-09-02 | Binance spot | 70 × `resolve: Host not found`, 3 × reset | 73 symbols cut, refilled by a later run |
| 2026-09-05 | Bybit spot | 12 × transport | cut, partly refilled 09-06 |
| 2026-09-06 | Lighter | 5 × reset | AERO/LIT/XCU 1m cut, ARM/KORU never created |
| 2026-09-06 | Bybit spot | 6 × reset | the six above |

### Defect 2 — relisting gap

Classifying every Bybit spot 1m file (live vs backup vs the venue's
`instruments-info` status) found eight *trading* symbols with a stale 1m
tail. Three were the resets above. The other five — GRAMUSDT, LUNCUSDT,
SHIBUSDT, USTCUSDT, ZKJUSDT — had never been reported as failing: each
run logged `CSV file for symbol: SHIBUSDT updated` within one second and
wrote nothing, their live and backup files were identical to the row, and
their 1h files were current.

The cause is in how Bybit answers a `start` that no candle satisfies.
Verified from both the VPS and a home connection, SHIBUSDT 1m:

| request | answer |
|---|---|
| `start=2021-11-29 06:11` (the file's last row) | 200 candles **02:52 .. 06:11** — the window *before* start |
| `start=2021-11-29 06:12` (tail + 1) | same window |
| `start=2021-12-05` | same window |
| `start=2021-12-06 00:00` | same window |
| `start=2021-12-07` | 200 candles 2021-12-07 00:00 .. 03:19 — the second listing |
| `start=tail+1&end=start+200m` | pre-gap window |
| `start=tail+1&end=start+20000m` | 200 candles ending at `end` (2021-12-13) |

So when nothing exists at or after `start`, Bybit does not return an empty
list; it returns its fixed window of the newest candles *before* `start`.
And `end`, when given, anchors that window: the answer is the newest
`limit` candles at or before `end`.

The client (`bybit-cpp-api/src/bybit_rest_client.cpp`,
`RESTClient::getHistoricalPrices`) paginated forward with `start` only and
treated a batch that wholly predates `from` as the end of the history:

```cpp
if (candles.front().startTime < from) {
    if (candles.back().startTime < from) {
        break; // Entire batch predates 'from'; no new data available.
    }
    ...
```

That is correct for a symbol that was delisted once and never came back.
For a relisted symbol, every `from` inside the gap between the two listings
draws the pre-gap window, the trim leaves one candle equal to the tail, the
writer drops it, `from` advances one interval, the next request draws the
same window, and `back() < from` ends the loop. Two requests, one second,
"updated", nothing written — forever. The 1h files escaped only because
their tails were current, so `from` never landed inside the gap.

Each of the five stuck 1m tails is the last candle of the symbol's first
listing; the Tardis `available_since` of each is the relisting date, one
to two weeks later:

| symbol | 1m frozen at | relisted (`available_since`) |
|---|---|---|
| SHIBUSDT | 2021-11-29 | 2021-12-06 |
| LUNCUSDT | 2022-05-12 | 2022-05-26 |
| USTCUSDT | 2022-05-12 | 2022-05-26 |
| ZKJUSDT | 2024-06-10 | 2024-06-13 |
| GRAMUSDT | 2026-06-16 | 2026-06-22 |

Hypotheses ruled out on the way, for the record: a stale `endTimestamp`
from the delisted-symbol path (spot status parsing handles `status`, and
the public.bybit.com archive for all five runs to 2026-09-05); a mis-read
tail (`lastValidRecord` accepts the scientific-notation rows the old
binary wrote); a regional difference in the venue's answer (the VPS and a
home connection get the same window).

## Fix

### Retry (root repo, `8ab7395`)

`include/stonky/transient_error.h` — `isTransientTransportError(message)`,
a substring classifier for the messages the transport layers actually emit:
`Transport failure`, `Connection reset`, `Broken pipe`, `timed out`,
`Timeout`, `Host not found`, `Temporary failure in name resolution`,
`handshake:`, `End of file`, `Operation canceled`, `Connection refused`,
`Network is unreachable`, `asio.`. A venue's own rejection (`10001`,
`-1121`, `internal error`, a CSV write failure) is deliberately not matched.

Every one of the six retry loops (Bybit, Lighter, Hyperliquid × candles,
funding) gained one branch:

```cpp
} else if (isTransientTransportError(errMsg) && attempt < maxRetries - 1) {
    const int waitMs = 2000 * (1 << attempt);      // 2, 4, 8, 16 s
    spdlog::warn("Transient network error for symbol: {}, retry {}/{} in {} ms: {}", ...);
    std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));
}
```

`maxRetries` stays 5; the rate-limit branch is unchanged (Lighter keeps its
fixed 60 s WAF cooldown). `test/transient_error_test.cpp` checks the
classifier against the verbatim log messages that caused the damage and
against the venue rejections that must not be retried (CTest target
`transient_error_classifies_network_faults`).

### Gap crossing (`bybit-cpp-api`, `01e0b54`)

The private request builder accepts an optional `endTime` and sends `end`
when it is positive. In the public loop, the "entire batch predates `from`"
case now probes forward instead of breaking:

```cpp
const auto windowMs = (limit - 1) * intervalMs;          // [from, windowEnd] = exactly limit slots
for (window = 0; window < 2000 && from <= to; ++window) {
    const auto windowEnd = std::min(to, from + windowMs);
    candles = fetch(start = from, end = windowEnd);      // venue answers the newest <= windowEnd
    if (!candles.empty() && oldest(candles) >= from) { found = true; break; }
    from = windowEnd + 1;                                 // window proven empty, step past it
}
if (!found) break;                                        // nothing at or after 'from' up to 'to'
```

A window is exactly `limit` slots wide, so the first window that holds any
candle at or after `from` holds all of them — no hole can be introduced.
An empty window is skipped whole. The bound of 2000 windows is 277 days at
1m and 45 years at 1h; a listing gap is days (SHIB 8, LUNC/USTC 14, ZKJ 3,
GRAM 6), so the five stuck symbols need 22–101 probes each, once. The batch
then continues down the existing trim → pop → write path, and a further
gap later in the history is handled the same way on the next iteration.

For a symbol delisted once and never relisted, `to` is already bounded by
its last trade, so the probe ends immediately and behaviour is unchanged.

## Covered / not covered

| | retry | gap crossing |
|---|---|---|
| Bybit | yes | yes (spot; the same client serves linear) |
| Lighter | yes | not needed — API serves full depth, no relisting cases seen |
| Hyperliquid | yes | not needed — retention-bounded, no relisting cases seen |
| Binance | **no** — no retry loop exists in the downloader | not needed — the venue serves nothing before a relisting (USDSUSDT), the older history exists only in backups |
| OKX | **no** — same | not needed — bulk archive, whole ranges |
| MEXC | **no** — same | not needed — serves nothing before `firstOpenTime` (AKITAUSDT and 21 others), older history only in backups |

Binance, OKX and MEXC still end a symbol on the first network fault; the
symbol is reported by name in the run summary and its CSV resumes from the
tail on the next run. Decision (operator, 2026-09-06): leave it. On these
three venues a fault is a delay, not a loss — Binance appends from the
tail, MEXC's transactional staging writes nothing on a failed run and
redoes it next time, OKX fetches archive files by day and picks up the
missing day — and no venue with a retention window short enough for a
week's delay to matter is affected (Hyperliquid 1m, 3.5 days, is not
downloaded). Adding six more retry loops would buy only a quieter exit
code. The Bybit / Lighter / Hyperliquid loops were different: they already
existed and were retrying the wrong thing.

## Operational follow-up

1. Rebuild and deploy 2.7.7.
2. Run `update_bybit_spot.sh` once. It refills the three reset symbols
   (MEWUSDT, SUIUSDT, TRXUSDT — the venue serves their full history, verified
   from 2025-03 / 2024-01 / 2025-08 respectively) and crosses the gap for the
   five relisted ones. Expect `Transient network error … retry n/4` warnings
   if Bybit resets again; expect no `Connection reset` in the final summary.
3. Verify the eight 1m files (tail current, no hole at the former cut) before
   deleting `/data/crypto_backup/bybit/spot`.
4. Ten delisted symbols whose live 1m was cut by a reset and cannot be
   re-downloaded (AGIX, GAL, MUSD, SPEC, SWEAT, TOMI, TRIBE, USDQ, USDR, ZEC)
   are restored from the backup, which is a strict superset for each; the
   backfill tool only prepends, so their live files were removed first and
   then copied whole.
5. The relisted five are frozen in the backup too — after step 2 the live
   files will be the first complete copies; do not "restore" them from the
   backup.
