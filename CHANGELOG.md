# Changelog

## Unreleased

- Add feature `tokio`: `Scheduler::run_until(clock, resolution, stop)` drives ticks
  until `stop` completes, then stops admission (ADR 0020).
- Add `JobDeclaration::first_run_after_interval`: opt-in delay of the first
  invocation by one interval (default still runs on the first tick).
- Add clock-driven job declarations, validation, misfire behavior, and overlap control.
