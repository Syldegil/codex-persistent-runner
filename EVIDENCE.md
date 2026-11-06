# Evidence

Historical 2026-11-06: nine unit tests passed; synthetic terminal-exit, interrupted-child and worker-restart scenarios passed. A first crash attempt timed out; corrected scenario completed in two attempts without rewriting its durable marker. Simulated network failure exhausted three attempts; simulated authentication failure blocked on the first. Real CLI validation initially failed sandbox startup; after sandbox policy changes, a real task and a two-attempt same-session resume were reported successful with file/checkpoint verification. These are historical private-service observations, not qualification of the exported portable unit.

New export tests are isolated fixtures and recorded in VALIDATION.md. No real CLI task was run for this publication. Reboot, SSH disconnection, screen locking and auth renewal remain NOT_TESTED. A receipt can reconcile a completed attempt after a queue commit gap; it cannot make arbitrary external effects exactly-once.
