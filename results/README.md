# Recorded benchmark output

Everything here is raw output from the `bench` job in `.github/workflows/ci.yml` (GitHub Actions `ubuntu-latest`,
release build). Each directory contains `environment.txt` (runner image, CPU, rustc, commit), three runs of
`tick_to_egress` and one run of `bench_dispatch`.

| Directory | Commit | What it is |
| :--- | :--- | :--- |
| `before-find-subsequence-fix/` | `81bc618` | First run of the rewritten benchmark; parse was 4.9 us because of a slow substring search |
| `latest/` | `587ebf6` | After the search fix. Numbers quoted in `website/docs/benchmarks/tick-to-egress.md` |

Commands: `cargo bench -p distribution --bench tick_to_egress` and `cargo bench -p distribution --bench bench_dispatch`.
The two directories ran on different runner VMs (EPYC 7763 vs EPYC 9V45), so compare stage by stage with that in mind.
