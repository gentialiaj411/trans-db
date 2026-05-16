# AGENTS.md

## Mission
Keep analysis token-efficient and evidence-based. Avoid broad scans unless explicitly requested.

## Startup Order (Always)
1. Read `PROJECT_STATE.md`.
2. Read `CLAIMS_MATRIX.md`.
3. Read `NEXT_TASK.md`.
4. Read only the minimum source/test/doc files needed to verify the current task.

## Scope Discipline
- Do not run full-repo audits by default.
- Do not claim features are complete unless directly verified.
- Mark uncertainty as `TODO/VERIFY`.
- Prefer targeted `rg` lookups and small file reads over directory-wide inspection.

## Editing Rules
- Prefer small diffs.
- Do not modify source/tests/build files/README unless the task explicitly requires it.
- Keep documentation concise and high-signal.
- Preserve existing architecture notes unless contradicted by evidence.

## Evidence Rules
- Treat `README.md` and `CLAUDE.md` as claims, not proof.
- For each important claim, capture evidence status in `CLAIMS_MATRIX.md`.
- If evidence is missing, add a narrow verification step instead of guessing.

## Validation Rules
- Run narrow tests only (single suite/case) when needed.
- Avoid expensive builds/benchmarks unless explicitly requested.
- Prefer quick file existence/command checks first.

## Handoff Rules
After meaningful work:
1. Append a short entry to `AUDIT_LOG.md` (date, what changed, key finding).
2. Update `NEXT_TASK.md` to one clear next step.
3. Keep unresolved items labeled `TODO/VERIFY`.
