# Screenshot Appendix — commands, sources, order-ID registry

Every figure in documents 01/02 maps to a command below. Run, screenshot the terminal, paste under the matching heading. `U=~/uat-stack` for UAT figures; `ssh manthan-prod` + `~/.pm2/logs/*.log` for mock-session figures. All logs are also archived in `docs/audit/uat-nse-2026-08-25/logs/`, `docs/audit/uat-nse-2026-08-24/`, `docs/audit/mock-drill-2026-08-22/logs/`.

## Document 01 (UAT Market Session)

| Fig | Command |
|---|---|
| PC-1 | `grep --color=always -E "SL deferred — intended 20% below DPR band\|$" <(grep "SL deferred" $U/logs/trade-execution.log \| tail -3 \| cut -c1-220)` |
| PC-2 | (captured 2026-08-24) `ssh manthan-prod 'jq -r ".orders[] \| select(.ordId==\"NZVAH00002H8\" or .ordId==\"NZVAH00003H8\" or .ordId==\"NZVAH00007H8\" or .ordId==\"NZVAH00008H8\") \| [.ordId,.exchOrdId,.symbol.dispSym,.ordAction,.ordType,.price,.qty,.status,(.rejReason\|.[0:65])] \| @tsv" ~/uat-orderbook-ND03920-final.json \| column -t -s "<TAB>"'` |
| QC-1 / OV-1 | `grep --color=always -E "invested=[0-9.]+\|$" <(grep "Entry order generated" $U/logs/rules-engine.log \| tail -12 \| cut -c1-200)` |
| QC-2 / OV-2 | as PC-2 with ordIds NZVAH00004H8 / NZVAH00005H8 |
| TP-1 | `grep --color=always -E "Entry timeout — modifying price\|retries exhausted\|MARKET fallback\|$" <(grep -E "Entry timeout\|retries exhausted\|MARKET fallback" $U/logs/trade-execution.log \| tail -12 \| cut -c1-240)` |
| EL-1 | `grep --color=always -E "margin pre-check failed\|DLQ\|$" <(grep -E "margin pre-check\|DLQ" $U/logs/trade-execution.log \| tail -4 \| cut -c1-240)` |
| SI-1 | `grep --color=always -E "strategies\|Manthan strategy loaded\|DEPLOYED\|$" <(grep "POST.*path=/api/v1/strategies$" $U/logs/api-gateway.log \| tail -1; grep "Manthan strategy loaded" $U/logs/rules-engine.log \| tail -1; psql -h localhost -U postgres -d trading_db -At -c "select 'LIFECYCLE: '\|\|event_type\|\|' '\|\|coalesce(details::text,'')\|\|' '\|\|created_at from strategy_lifecycle_events order by id")` |
| SI-2 | `grep --color=always -E "place-order\|Requested\|$" <(grep -E "place-order" $U/logs/trade-execution.log \| grep -v modify \| tail -12 \| cut -c1-240)` + `psql … select … from manthan_orders where created_at::date='2026-08-25' order by id` |
| MS-1 | `grep --color=always -E "predates strategy\|$" <(grep -i predates $U/logs/rules-engine.log \| tail -2 \| cut -c1-220)` |
| MS-2 | `grep --color=always -E "dedup OK\|$" <(grep "dedup OK" $U/logs/trade-execution.log \| tail -4 \| cut -c1-200)` |
| MS-3 | `grep --color=always -E "Reconciler fixed order\|pass complete\|$" <(grep -E "Reconciler" $U/logs/trade-execution.log \| tail -11 \| cut -c1-200)` |
| MS-4 | `grep --color=always -E "too close to market close\|$" <(grep "too close to market close" $U/logs/trade-execution.log \| tail -2 \| cut -c1-220)` |
| PS-1 | `grep --color=always -E "deactivate\|PAUSED\|$" <(grep deactivate $U/logs/api-gateway.log \| tail -1; psql -h localhost -U postgres -d trading_db -At -c "select 'LIFECYCLE: '\|\|event_type\|\|' '\|\|created_at from strategy_lifecycle_events where event_type='PAUSED'")` + zero-TCS queries |
| RS-1 | as PS-1 with `activate` / `RESUMED` + INFY order row |
| TC-1 | `grep --color=always -E "LIVE BUY filled\|Executed\|SL deferred\|$" <(grep RELIANCE $U/logs/trade-execution.log \| grep -E "place-order\|fallback\|filled\|SL" \| tail -12 \| cut -c1-220)` |

## Document 02 (Mock Market Session — run on manthan-prod, logs `~/.pm2/logs/*-error.log`)

| Fig | Source (all already captured 2026-08-22, timestamps IST) |
|---|---|
| M-PC-1 | TE log 08:05:04 UTC `Entry on hold — stock at upper circuit` (AXISBANK ×4 users) + `manthan_orders` 0-rows query |
| M-PC-2 | TE log 07:29:42 `SL deferred — intended 20% below DPR band` + order 8917 `SL_DEFERRED_BAND intended=1262.56 < dpr_lower=1381.80` |
| M-PC-3 | order book rows NYMZX00295F8 (BSE closing-price rule) / NYMZX00297F8 (NSE price freeze) |
| M-QC-1 | TE log 08:07:59 `margin pre-check failed … required=₹1119318750.00` + events 8951/8952 `MARGIN_INSUFFICIENT` + `Inbox row → DLQ class=POISON` |
| M-QC-2 / M-OV-1 | order book NYMZX002A6F8 / NYMZX002A7F8 `Maximum Quantity (43417)` + `manthan_signal_decisions` intended_invested rows |
| M-TP-1 | order 8915 events `MODIFIED retry 1, drift 0.45%, escalation 0.20%` |
| M-EL-1 | TE log 07:29:26 `get-fund-limit → availableMargin` + M-QC-1 |
| M-SI-1 | 06:59:11 UTC four-layer create trail (gateway/user-config/DB/rules-engine) |
| M-SI-2 | TE log place-order → `{"ordId":"NZWKE00001F8","ordStatus":"Requested"}` |
| M-MS-1 | orders 9197–9208 `SL_PLACED` NYMZX0028CH8…297H8 (2026-08-24 12:23–12:24 IST) + broker book Pending rows |
| M-MS-2 | TE log `Reconciler fixed order → CANCELLED` (order 8861) + `Reconciler pass complete` |
| M-MS-3 | RE log 06:59:11 `Strategy created outside market hours — deferring entries` |
| M-MS-4 | RE log `Trailing SL updated` (IPCALAB 07:33:57) + TE `SL_DEFERRED_TRAIL` event (order 27) |
| M-PS-1 | 08:32:23 deactivate + `PAUSED` + CUB-while-paused zero-order evidence |
| M-RS-1 | 08:35:10 activate + `RESUMED` + NRBBEARING order 8974 |
| M-TC-1 | RELIANCE chain 07:29:26–42: orders 8915 events PLACED→MODIFIED→FILLED `WSS fill confirmed`, exchOrdId 1310000014940827 |

## Order-ID registry

| Session | Series | Range used | Exchange order numbers |
|---|---|---|---|
| Mock 2026-08-22 (prod middleware) | NZWKE…F8 (ND03920), NYMZX…F8 (S4450) | NZWKE00001F8–0001BF8, NYMZX0027DF8–002ABF8 | 1010000000000363 – 1310000014943205 |
| UAT 2026-08-24 (broker-level) | NZVAH…H8 | NZVAH00001H8–00009H8 | 1300000000047686 – …047940 |
| UAT 2026-08-25 (pipeline) | NZVAH…I8 | NZVAH00001I8–00018I8 | sim-assigned |

## Known observations disclosed with the submission

1. Stop/square-off of strategy positions — engineering fix in progress (lifecycle `DELETED` records correctly; positions remain SL-protected).
2. UAT simulator fills large-cap instruments only; small/mid-cap orders exercised the full lifecycle via sim cancellation + reconciler.
3. GTC validity downgraded to DAY by the OMS (broker-side observation, raised with Indira).
