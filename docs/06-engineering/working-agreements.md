# Development workflow and coaching agreement

## Daily development cycle

1. Start from the ticket specification in `docs/03-planning/tasks/MLP-XXX.md`.
2. Confirm dependencies; select a minimal vertical increment.
3. Implement the first solution yourself.
4. Write unit tests for invariants and at least one failure mode.
5. Review code for typing, error paths, timeouts, cancellation, metrics and operability.
6. Capture verification evidence and change the ticket status in `docs/03-planning/backlog.csv` and `docs/03-planning/progress.md`.
7. Finish with a concise 2–3-minute explanation of technical tradeoffs.

## Code quality norms

- Prefer `src/`-layout Python packages, explicit types and `pytest`.
- Separate domain models from transport/database/runtime adapters.
- Keep API handlers thin; execution must be outside HTTP workers.
- Use migrations for schema changes, not destructive startup `create_all` in later milestones.
- Include retry/backoff budgets, bounded concurrency, timeouts and structured error types.
- Introduce dependencies at the milestone where they are needed, not all on day one.
- Run `ruff`, unit tests and relevant integration tests before marking a ticket done.
- Avoid hidden cloud credentials and public endpoints with expensive GPU capacity.

## Suggested commit convention

`feat(MLP-001): implement job state transition rules`  
`test(MLP-005): cover cancellation race and idempotency`  
`docs(MLP-016): explain GPU scheduling constraints`

## Working with AI assistants

Use the assistant as coach and reviewer, not as an automatic replacement for practicing Python. Request targeted prompts, adversarial tests and architecture critiques. Preserve ownership: you should be able to explain every design decision and debug the code without an assistant.
