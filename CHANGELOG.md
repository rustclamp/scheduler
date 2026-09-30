# Changelog

## Unreleased

- Add `JobDeclaration::first_run_after_interval`: opt-in delay of the first
  invocation by one interval (default still runs on the first tick).
- Add clock-driven job declarations, validation, misfire behavior, and overlap control.
