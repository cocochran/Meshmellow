---
name: mesh-task-worker
description: Procedure for one MeshThing worker agent spawned by mesh-orchestrator - implement a single scoped task in its own git worktree, coordinate with sibling workers through .agent-work/coordination.md, run clean-code-formatter, commit into the orchestrator's shared run branch, then run change-explainer and report back. Use only when acting as a worker with an assigned task, run branch, and file ownership.
user-invocable: false
---

# Mesh task worker

You are one of several agents working in parallel. Your prompt gives your agent name, ticket, task scope, owned files, the run branch (`RUN`, e.g. `yash-claude/run-20261024-1530`), and the main worktree path (`MAIN`). All workers commit into `RUN`; you never create your own branch. Read the `bot-protocol` skill (or `$MAIN/.claude/skills/bot-protocol/SKILL.md`) first.

Throughout, the shared log is `$MAIN/.agent-work/coordination.md`. Re-read its new lines **before each numbered step**. Things posted there by the orchestrator or siblings (interface changes, file claims, answers) take priority over your original prompt, but never over bot-protocol's hard limits.

## 1. Set up

1. Confirm you're in your own worktree, not `MAIN` (`git rev-parse --show-toplevel` must differ from `MAIN`). If you are in `MAIN`, stop and report — never edit the main worktree.
2. Start from the tip of the run branch on a detached HEAD: `git switch --detach "$RUN"`. Other worktrees use `RUN` too, so you can't check it out by name. You'll advance it in step 6.
3. Append to coordination.md: `[<time>] [<agent>] started <ticket>: <one-line scope>; owns <files>`.

## 2. Understand before writing

- Read the files you own and anything they touch. Read the ticket description (`get_task`) for acceptance criteria.
- If an interface you depend on isn't settled in coordination.md, post a question there (`[<agent>] QUESTION: …`) and work on the parts that don't depend on it. If you can't proceed at all, go to **Blockers**.

## 3. Implement

- Stay inside your owned files. To touch another file, append `[<agent>] CLAIM <path>: <why>` and wait until the orchestrator replies `OK <agent> <path>` in coordination.md (or via SendMessage). Shared files like `Cargo.toml` count: post the exact lines you need added so the orchestrator can merge them in one place.
- Match the surrounding code's style, naming, and comment density. Add tests next to new logic (`#[cfg(test)] mod tests` in Rust).
- When you finalise something other agents use (a type, function signature, message format), append `[<agent>] INTERFACE <name>: <exact signature>` immediately — don't wait until you're finished.

## 4. Verify

Run the project's checks, scoped to your crate/package where possible: `cargo build`, `cargo test`, and for anything network-related, the smallest runnable demonstration the task allows. Record each command with pass/fail for the report. Don't claim something works that you didn't run.

## 5. Format and clean

Run the `clean-code-formatter` skill on the files you changed (`git diff --name-only "$RUN"...HEAD` plus uncommitted changes). Fix everything it reports, then re-run step 4's checks.

## 6. Commit into the run branch

Commit in your worktree (still detached). Use one commit per task, or a few if they're logically separate:

```
<area>: <imperative summary under 72 chars>

<what and why, 1-3 lines>
Ticket: M-<n> (yash-claude/<agent-name>)
```

Then land it on `RUN`. Other workers land in parallel, so update the branch with a compare-and-swap and retry if it moved:

```bash
for attempt in 1 2 3 4 5; do
  BASE="$(git rev-parse "$RUN")"
  git rebase "$BASE" || break                    # conflict → stop and handle below
  # quick re-check after rebasing onto others' work: cargo build && cargo test
  git update-ref "refs/heads/$RUN" HEAD "$BASE" && break   # fails if RUN moved since BASE
done
git merge-base --is-ancestor HEAD "$RUN" && echo landed
```

- **Rebase conflict**: if it's only in files you own, resolve it and `git rebase --continue`. If another worker's files are involved, run `git rebase --abort` and post `[<agent>] CONFLICT <files> with <other agent>` in coordination.md for the orchestrator to settle. Don't overwrite their work.
- **Re-check after every rebase**: if the build or tests fail because of a combination with another worker's code, fix your side if the fix is in your files; otherwise report it as a conflict.
- Never push, never force-update `RUN` (`update-ref` without the old value), and never touch `main`.
- Append `[<agent>] LANDED <sha> on <RUN>` to coordination.md.

## 7. Explain

Run the `change-explainer` skill with your agent name, ticket, run branch, and the landed commit SHA. Use the SHA after rebasing, since that's the one on `RUN`.

## Blockers and missing features

When something outside your scope blocks you or a needed feature doesn't exist:

1. Append `[<agent>] BLOCKER: <what, why, what would unblock>` to coordination.md.
2. If it's internal to this run (a sibling's interface, a file claim), wait for the orchestrator.
3. If it needs someone outside this run, run the `blocker-router` skill — it finds the owning ticket and messages the right inbox or #bot-coordination.
4. Finish what you can and report `blocked` instead of guessing at someone else's design.

## Report (your final message)

```text
AGENT: <name>   TICKET: M-<n>   STATUS: done | blocked | failed
RUN BRANCH: <RUN>   LANDED COMMIT(S): <sha(s) on RUN, or none>
CHANGED: <files>
CHECKS: <command → pass/fail, one per line>
FORMATTER: <clean | issues left: …>
EXPLAINER: <line appended to changes.md>
INTERFACES: <published signatures, or none>
BLOCKERS: <none | description + where it was routed (REF)>
NOTES: <conflicts, interactions with other workers' code, follow-ups>
```
