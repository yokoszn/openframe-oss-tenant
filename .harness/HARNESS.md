# Harness — Rebuild the frontend image with NEXT_PUBLIC_TENANT_HOST_URL=http://10.0.64.11 so the dashboard data hooks resolve correctly. Patch the chunks in the running container.

repo: /home/ctown/projects/openframe-sangdb · phase: Planning

status: **FRESH**

## Constraints (numbered, prioritized — rank 0 is a refusal)

- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.env`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.pem`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.key`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.p12`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.pfx`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `credentials`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `secrets/`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.ssh/`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.aws/`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.gnupg/`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.git/`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.claude/`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.agents/`
- **FLOOR** (rank 0) [machine floor] Never edit/write anything whose path contains `.config/opencode`
- **C2** (rank 0) [/home/ctown/.config/opencode/AGENTS.md] A class of solution that failed once is never re-proposed; three failed
- **C3** (rank 0) [/home/ctown/.config/opencode/AGENTS.md] Never run the heavy path when the cheap path answers the question.
- **C4** (rank 0) [/home/ctown/projects/openframe-sangdb/CLAUDE.md] - Use components from `@flamingo-stack/openframe-frontend-core` — never create custom UI primitives
- **C6** (rank 0) [/home/ctown/projects/AGENTS.md] `song/AGENTS.md` line 5 already points at it (`鍵 ∈ ../AGENTS.md`); it had never been written, so
- **C11** (rank 0) [/home/ctown/projects/AGENTS.md] flat spectrum is a finding worth reporting in one line. What is forbidden is the twelfth variant of
- **C13** (rank 0) [/home/ctown/projects/AGENTS.md] layer**. A trained lift is permitted only as a *named second lift* (the boost face), never the
- **C17** (rank 0) [/home/ctown/projects/AGENTS.md] > on* — not that learning the encoder is forbidden.
- **C18** (rank 0) [/home/ctown/projects/AGENTS.md] 1. **A class of solution that failed once MUST NOT be re-proposed.** Write the failure down with
- **C20** (rank 0) [/home/ctown/projects/AGENTS.md] job is to find which rung the work is on and **stand on the ones below, never re-derive them**.
- **C23** (rank 0) [/home/ctown/projects/AGENTS.md] 3. Never gate a build on a `ps aux | grep cargo` check. The `cargo` process is the lock; you do
- **C31** (rank 0) [/home/ctown/projects/CLAUDE.md] - `bun.lock` — Lock file (use Bun, never npm)
- **C34** (rank 0) [/home/ctown/projects/CLAUDE.md] - **Never blame user setup without checking actual config first** — Read files, check environment, verify commands actually work before claiming something is invalid
- **C36** (rank 0) [/home/ctown/projects/CLAUDE.md] - **Never gaslight** — Don't repeat the same false diagnosis multiple times; actually debug when the user corrects you
- **C12** (rank 1) [/home/ctown/projects/AGENTS.md] - **FACE-2** — the lift MUST be a fixed type choice (the applicative alpha), **not a trained
- **C14** (rank 1) [/home/ctown/projects/AGENTS.md] - **REG-4** — a new architecture must beat the reservoir baseline (random Fourier features +
- **C25** (rank 1) [/home/ctown/projects/CLAUDE.md] When working in this monorepo, always verify you are in the correct project directory before making any file changes. Run `pwd` and `ls` to confirm context before editing Cargo.toml, package.json, or any config files.
- **C26** (rank 1) [/home/ctown/projects/CLAUDE.md] Primary languages: TypeScript and Rust. Package manager: Bun (not npm). When generating code, use RS256 for JWT validation (not HS256). Always include all necessary imports in generated Rust code.
- **C1** (rank 2) [/home/ctown/.config/opencode/AGENTS.md] If a guard ever passes something it should refuse, fix the guard — do not
- **C5** (rank 2) [/home/ctown/projects/openframe-sangdb/CLAUDE.md] - Apache NiFi (removed from project - NOTE: Some NiFi dependencies may still exist in pom.xml but should not be used)
- **C7** (rank 2) [/home/ctown/projects/AGENTS.md] a theorem in `sanguine-refound-s/proof/` saying so. You do not "implement a transformer" or "build
- **C8** (rank 2) [/home/ctown/projects/AGENTS.md] theorem for the algebra; the reading is `L1` and earns lower billing (`VERF-3` — do not spend the
- **C9** (rank 2) [/home/ctown/projects/AGENTS.md] the choice between them is the only place judgement lives. **Sampled**: for inputs that do not
- **C10** (rank 2) [/home/ctown/projects/AGENTS.md] lift and Torch-JEPA. Prefer solved when the data has a coupling to fold; `cone_fraction` tells
- **C15** (rank 2) [/home/ctown/projects/AGENTS.md] (`cone_fraction`; you get a variable per mode and you do not want them all — cut `r` where the cone
- **C16** (rank 2) [/home/ctown/projects/AGENTS.md] Model Zoo` at once. Settled, already measured — do not re-derive it:
- **C19** (rank 2) [/home/ctown/projects/AGENTS.md] 2. **Three failed attempts on one problem ends the attempts.** Stop and report the boundary. Do not
- **C21** (rank 2) [/home/ctown/projects/AGENTS.md] **Do NOT check for other build processes before building.** Run the build directly. Standing rule:
- **C22** (rank 2) [/home/ctown/projects/AGENTS.md] 2. If the sandbox target dir is in use (`/home/ctown/projects/.agent-work/cargo-target/`), do not
- **C24** (rank 2) [/home/ctown/projects/AGENTS.md] 4. Do not verify the binary timestamp moved after a build. If the build's exit code is 0, treat
- **C27** (rank 2) [/home/ctown/projects/CLAUDE.md] Key projects: AMIE (Rust cloud management API), MSSP portal (o2s-operator for service ops, CreatureGRC for security/GRC), OpenFang, frontend-tascloud (TypeScript/Bun). The marketing site and the portal are separate projects — do not confuse them.
- **C28** (rank 2) [/home/ctown/projects/CLAUDE.md] When the user gives you a plan or task list, execute it immediately. Do not spend time on setup overhead (skill loading, worktree creation, environment scanning) unless explicitly asked. Bias toward action over preparation.
- **C29** (rank 2) [/home/ctown/projects/CLAUDE.md] - Read config files, test commands, check actual error messages — don't assume
- **C30** (rank 2) [/home/ctown/projects/CLAUDE.md] - If you're wrong about something, acknowledge it and fix it; don't repeat the same false diagnosis
- **C32** (rank 2) [/home/ctown/projects/CLAUDE.md] archived. Do not work on them. AI tooling work continues in Rust under `ai-projects/` (AMIE).
- **C33** (rank 2) [/home/ctown/projects/CLAUDE.md] Do NOT cycle through speculative fixes. When debugging, state your hypothesis, test it with a single minimal change, and verify before moving on. If 3 attempts fail on the same issue, stop and ask the user for more context rather than trying a 4th guess.
- **C35** (rank 2) [/home/ctown/projects/CLAUDE.md] - **Test commands directly** — If something fails, run the exact command and see the actual error, don't assume

## Verification steps (all must be green before DONE)


## The loop

1. HARNESS: this file exists before any action.
2. PLAN: write the plan against the constraints (cite C-ids).
3. EXECUTE: one step at a time.
4. VERIFY: run `syndesi harness verify` after each step; red → REPLAN, never retry blind.
