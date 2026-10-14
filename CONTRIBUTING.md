# Contributing

## Workflow

1. `main` is protected. All changes arrive through a pull request.
2. Branch from `main`: `feature/<topic>`, `fix/<topic>`, `docs/<topic>`,
   `refactor/<topic>`, `chore/<topic>`.
3. One focused change per PR. Describe what, why and how to verify.
4. Use Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`, `test:`.
5. Merge with **Squash and merge**, then delete the branch.

## Rules

- Never commit secrets. Only `.env.example` (safe example values) is tracked.
- Every new variable is added to `.env.example`.
- Significant design changes get an ADR in `docs/decisions/`.
