# GitHub runner, 2026-10-03

The same harness as the laptop run, from `.github/workflows/bench.yml` (run 37153211684), commit 3cb696c plus the image
fix.

## Machine

- GitHub-hosted `ubuntu-latest`: AMD EPYC 7763, 4 vCPUs, Linux 6.17 (Azure), Python 3.14.8.
- Postgres 18.6 (alpine) as a service container on the same runner, default settings.
- Four worker processes, 64 concurrent workflows each, a pool of 10 connections each. Workers, Postgres and the
  harness share the 4 vCPUs, so this is a crowded machine, not a tuned one.

## Results

| run | load | result |
|---|---|---|
| drain | 5,000 workflows x 3 steps | 5.8 s, 863 workflows/s, 2,589 steps/s |
| latency | 200/s for 20 s, 3 steps, open loop | p50 5 ms, p95 10 ms, p99 170 ms, max 404 ms |
| replay | 100 / 1,000 / 10,000 recorded steps | 0.09 / 0.97 / 9.65 ms per wake, about 1 us per step |

## Reading it next to the laptop run

Throughput is about the same as the laptop's (896 workflows/s) on a quarter of the cores, and the median latency is a
quarter of the laptop's (5 ms against 22 ms): on Linux there is no Docker Desktop network bridge between the workers and
Postgres, and each workflow is a handful of round trips. The p99 is close on both (170 and 203 ms) and has not been
profiled; the leading suspect is every idle worker waking on every `NOTIFY` and racing for the same rows.
