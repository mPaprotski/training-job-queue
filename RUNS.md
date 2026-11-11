# All preserved chaos and load runs

Generated with `python scripts/index_runs.py`. Counts include deliberate
negative probes, calibration, packaging checks and diagnostic repeats.
Historical versions/configurations differ; their successes are not all
evidence for the current source. Raw files are never rewritten.

Total: **11 chaos runs** (6 full 10,000-job runs), **20 load runs**.

| Run | Type | Result | Expected jobs | Source revision |
| --- | --- | --- | ---: | --- |
| [chaos-20261109T215536Z-seed20261110-d70f529a](results/chaos-20261109T215536Z-seed20261110-d70f529a/summary.json) | chaos | PASS | 1000 | uncommitted/not supplied |
| [chaos-20261109T215728Z-seed20261111-f00f6650](results/chaos-20261109T215728Z-seed20261111-f00f6650/summary.json) | chaos | FAIL | None | uncommitted/not supplied |
| [chaos-20261109T215907Z-seed20261110-0c5fe935](results/chaos-20261109T215907Z-seed20261110-0c5fe935/summary.json) | chaos | PASS | 1000 | uncommitted/not supplied |
| [chaos-20261110T072122Z-seed1-21b9879e](results/chaos-20261110T072122Z-seed1-21b9879e/summary.json) | chaos | PASS | 10000 | uncommitted/not supplied |
| [load-20261110T072810Z-seed1-803346f7](results/load-20261110T072810Z-seed1-803346f7/summary.json) | load | PASS | 54 | uncommitted/not supplied |
| [chaos-20261110T073008Z-seed2-e01392da](results/chaos-20261110T073008Z-seed2-e01392da/summary.json) | chaos | PASS | 10000 | uncommitted/not supplied |
| [chaos-20261110T073357Z-seed3-27f02072](results/chaos-20261110T073357Z-seed3-27f02072/summary.json) | chaos | PASS | 10000 | uncommitted/not supplied |
| [load-20261110T073539Z-seed1-cd64e0a2](results/load-20261110T073539Z-seed1-cd64e0a2/summary.json) | load | FAIL | 150 | uncommitted/not supplied |
| [load-20261110T074449Z-seed1-a94d1ca5](results/load-20261110T074449Z-seed1-a94d1ca5/summary.json) | load | FAIL | 1414 | uncommitted/not supplied |
| [load-20261110T074741Z-seed1-bf720b8f](results/load-20261110T074741Z-seed1-bf720b8f/summary.json) | load | FAIL | 1443 | uncommitted/not supplied |
| [load-20261110T075003Z-seed1-e813a251](results/load-20261110T075003Z-seed1-e813a251/summary.json) | load | FAIL | 7050 | uncommitted/not supplied |
| [chaos-20261110T130219Z-seed11-15fdf8ea](verification/clean-room-001/harness/chaos-20261110T130219Z-seed11-15fdf8ea/summary.json) | chaos | PASS | 1000 | uncommitted/not supplied |
| [chaos-20261110T210117Z-seed21-cb2d0810](verification/review-fixes-003/harness/chaos-20261110T210117Z-seed21-cb2d0810/summary.json) | chaos | PASS | 1000 | 8db3f12-plus-local-review-fixes-steps-1-7 |
| [chaos-20261110T210241Z-seed22-36ad9483](verification/review-fixes-003/harness/chaos-20261110T210241Z-seed22-36ad9483/summary.json) | chaos | PASS | 10000 | 8db3f12-plus-local-review-fixes-steps-1-7 |
| [chaos-20261110T212015Z-seed31-ff161d72](verification/review-fixes-004/harness/chaos-20261110T212015Z-seed31-ff161d72/summary.json) | chaos | PASS | 10000 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [load-20261110T212432Z-seed41-f43fb79c](verification/review-fixes-004/harness/load-20261110T212432Z-seed41-f43fb79c/summary.json) | load | FAIL | 1447 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [load-20261110T212559Z-seed41-426a663f](verification/review-fixes-004/harness/load-20261110T212559Z-seed41-426a663f/summary.json) | load | FAIL | 2266 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [load-20261110T212815Z-seed41-19f98092](verification/review-fixes-004/harness/load-20261110T212815Z-seed41-19f98092/summary.json) | load | PASS | 1450 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [load-20261110T212932Z-seed41-0d1bf97a](verification/review-fixes-004/harness/load-20261110T212932Z-seed41-0d1bf97a/summary.json) | load | FAIL | 3498 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [load-20261110T213119Z-seed41-98fe2d9c](verification/review-fixes-004/harness/load-20261110T213119Z-seed41-98fe2d9c/summary.json) | load | PASS | 1450 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [load-20261110T213239Z-seed41-888b0d7b](verification/review-fixes-004/harness/load-20261110T213239Z-seed41-888b0d7b/summary.json) | load | FAIL | 7037 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [load-20261110T213602Z-seed41-95bf549b](verification/review-fixes-004/harness/load-20261110T213602Z-seed41-95bf549b/summary.json) | load | PASS | 7050 | 8db3f12-plus-local-review-fixes-steps-1-10 |
| [chaos-20261111T081108Z-seed51-4174187c](verification/review-fixes-006/harness/chaos-20261111T081108Z-seed51-4174187c/summary.json) | chaos | PASS | 10000 | 90cab34-plus-local-slot-fixes |
| [load-20261111T081516Z-seed61-2bf26d95](verification/review-fixes-006/harness/load-20261111T081516Z-seed61-2bf26d95/summary.json) | load | PASS | 1450 | 90cab34-plus-local-slot-fixes |
| [load-20261111T081633Z-seed62-0697c983](verification/review-fixes-006/harness/load-20261111T081633Z-seed62-0697c983/summary.json) | load | PASS | 7050 | 90cab34-plus-local-slot-fixes |
| [load-20261111T081753Z-seed63-e86c2947](verification/review-fixes-006/harness/load-20261111T081753Z-seed63-e86c2947/summary.json) | load | FAIL | 7050 | 90cab34-plus-local-slot-fixes |
| [load-20261111T081913Z-seed64-121f7b3a](verification/review-fixes-006/harness/load-20261111T081913Z-seed64-121f7b3a/summary.json) | load | PASS | 7050 | 90cab34-plus-local-slot-fixes |
| [load-20261111T082158Z-seed71-2df81268](verification/review-fixes-006/harness/load-20261111T082158Z-seed71-2df81268/summary.json) | load | PASS | 7050 | 90cab34-plus-local-slot-fixes |
| [load-20261111T082318Z-seed72-bc1e9a0f](verification/review-fixes-006/harness/load-20261111T082318Z-seed72-bc1e9a0f/summary.json) | load | PASS | 7050 | 90cab34-plus-local-slot-fixes |
| [load-20261111T082438Z-seed73-db806075](verification/review-fixes-006/harness/load-20261111T082438Z-seed73-db806075/summary.json) | load | PASS | 7050 | 90cab34-plus-local-slot-fixes |
| [load-20261111T082559Z-seed74-325d01ab](verification/review-fixes-006/harness/load-20261111T082559Z-seed74-325d01ab/summary.json) | load | PASS | 1450 | 90cab34-plus-local-slot-fixes |

Exact parameters and source hashes: [RUNS.json](RUNS.json).
Current verdict: [VERDICT.md](VERDICT.md). Current raw capacity evidence:
[verification/review-fixes-006/capacity-poll.json](verification/review-fixes-006/capacity-poll.json).
