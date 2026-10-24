---
name: mesh-orchestrator
description: Run a parallel MeshThing work session as yash-claude - read the inbox and Kaneo board, split tickets into independent tasks, spawn worker agents in separate worktrees that run in parallel and coordinate, poll the Discord inbox while they work, keep the task list current, then hand off to commit-helper. Use when Yash asks to work on MeshThing tickets with multiple agents or to "orchestrate" a run.
argument-hint: "[ticket numbers, e.g. 4 11 - defaults to Yash's in-progress tickets]"
disable-model-invocation: true
---

# Mesh orchestrator

You are the primary `yash-claude` agent. You plan, spawn, relay, and track; workers write the code. Load the `bot-protocol` skill first and keep its rules for the whole run.

Tickets requested: `$ARGUMENTS`

## 1. Start up

1. Resolve `MAIN` and `WORK` as in bot-protocol. Create `$WORK/` if missing and make sure `.agent-work/` is in `$MAIN/.gitignore` (add the line if not).
2. If `$WORK/plan.md` exists with unfinished rows, this is a resumed run: re-read it and `coordination.md` and continue from where it stopped instead of re-planning.
3. Run the `bot-inbox-check` skill once — handle mail before choosing work.
4. Make sure the working tree on `main` is clean (`git status --short`). If it isn't, stop and ask Yash; never stash or discard their changes.

## 2. Choose and claim tickets

1. Refresh the board: `list_tasks` for the MeshThing project.
2. Candidate tickets: the numbers in `$ARGUMENTS`; if none, the tickets assigned to Yash that aren't `done`.
3. Never take a ticket assigned to someone else. For an unassigned ticket, it's ours only after Yash approves it in step 3 **and** you've announced the claim in #bot-coordination (`TYPE: UPDATE`, `TO: all`) without an objection already in the channel.

## 3. Plan

Break each ticket into tasks that one agent can finish independently:

- Each task owns a disjoint set of files/modules. If two tasks must share a type or interface (e.g. the P2P message enum), make the interface its own first task, or write the agreed signature into `coordination.md` before spawning so both sides build against it.
- Prefer 2–4 parallel workers. More agents than independent pieces of work just produces merge conflicts.
- Name agents by role (`p2p-endpoint`, `msg-schema`, …). There is **one branch for the whole run**, `yash-claude/run-<yyyymmdd-hhmm>`, and every worker commits into it.

Write `$WORK/plan.md`:

```markdown
# Run <ISO date> — run branch: yash-claude/run-<yyyymmdd-hhmm>
| Agent | Ticket | Task | Owns (files) | Depends on | Status | Commit | Notes |
|---|---|---|---|---|---|---|---|
| msg-schema | M-4 | Message enum + serde | src/protocol.rs | — | planned | | |
```

Statuses: `planned → running → landed → done`, or `blocked` / `failed` (with the reason in Notes). `landed` means the worker's commit is on the run branch; `done` means the explainer line is written too.

Show Yash the plan with `AskUserQuestion` and get approval before anything outward-facing. In the same question ask whether this run may **push and open a PR** at the end (default: no — commit locally only). Record the answer at the top of `plan.md` as `Push/PR: yes|no`.

## 4. Announce and set up

1. Create the run branch from `main` **without checking it out**: `git branch yash-claude/run-<stamp> main`. The main worktree stays on `main` while workers run. Workers advance the run branch from their own worktrees, and a branch checked out in `MAIN` would end up out of sync with its working files.
2. Move the chosen tickets to `in-progress` (`update_task_status`). Assign an unassigned claimed ticket to Yash only if Yash approved that in step 3.
3. Edit our #bot-directory entry (find it with `read_messages`; post a fresh one if editing isn't possible) so `WORKING ON`, `BRANCH/WORKTREE`, and `ADDITIONAL AGENTS` list this run.
4. Post one `TYPE: UPDATE` to #bot-coordination naming the tickets and branch. Log it in `outbox.md`.
5. Start inbox polling: load `CronCreate` via ToolSearch and schedule `/bot-inbox-check` every 10 minutes for this session. Save the cron ID in `state.json`. If CronCreate isn't available, rely on the checks in step 6.

## 5. Spawn workers

Send **all independent workers in one message** (multiple Agent calls) so they run in parallel. For each: `subagent_type: "general-purpose"`, `isolation: "worktree"`, a short `description`, and this prompt (fill the placeholders):

```text
You are worker <agent-name> for yash-claude on MeshThing.
Main worktree: <MAIN>. Shared state: <MAIN>/.agent-work/.
Ticket: M-<n> "<title>" (Kaneo task id <id>).
Your task: <precise scope, acceptance criteria>.
You own: <files>. Do not edit files outside this list without first posting a claim in coordination.md and getting a reply from the orchestrator.
Interfaces agreed so far: <excerpt from coordination.md or "none">.
Run branch: yash-claude/run-<stamp>. Commit your work into it using the landing procedure in mesh-task-worker; don't create other branches, and never push.
Instructions: invoke the mesh-task-worker skill (or read <MAIN>/.claude/skills/mesh-task-worker/SKILL.md and follow it exactly).
Finish with the report format that skill defines.
```

Spawn dependent tasks only once what they depend on is `done`. Set each spawned row to `running`.

## 6. While workers run

You're re-invoked when a worker finishes or the cron fires. Each time:

1. **Worker finished** → read its report. Check its commit is on the run branch (`git merge-base --is-ancestor <sha> yash-claude/run-<stamp>`), then update its `plan.md` row (status, commit SHA, checks, blockers). If it failed, decide: re-spawn with a sharper prompt, fold the work into another task, or mark `blocked` and route it through `blocker-router`.
2. **Cron fired or between steps** → run `bot-inbox-check`. It updates `plan.md` with new or changed tasks and tells you which running workers are affected.
3. **Relay** anything a running worker needs (inbox answers, interface changes, another worker's findings) with `SendMessage` to that worker, and append it to `coordination.md`. Workers read `coordination.md` between steps, so the log is the fallback when SendMessage can't reach one.
4. Read new lines in `coordination.md`. Answer file-claim requests, settle interface disputes (record the decision there), and spawn follow-up tasks for missing pieces.
5. Keep Kaneo honest: if a ticket's scope changed, add a comment (`create_task_comment`) instead of rewriting someone else's description.

Don't spin while waiting: if nothing needs doing, end your turn and let the notification or cron wake you.

## 7. Integrate

When every row is `done` (or explicitly dropped with Yash's agreement), all work is already on the run branch:

1. Check out the run branch in the main worktree: `git switch yash-claude/run-<stamp>`. Only do this once no worker is running, since workers can't land while it's checked out here.
2. Run the full checks: `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` (or the equivalents for other languages that appear). Fix them yourself with a commit on the run branch, or switch `MAIN` back to `main` and spawn a fix worker, until they pass, or record which checks still fail.
3. Run the `commit-helper` skill. It writes the PR commit message, posts DONE to Discord, and moves the tickets to `in-review`.

## 8. Wrap up

1. Delete the inbox cron (`CronDelete` with the saved ID).
2. Run `bot-inbox-check` one last time.
3. Update the directory entry: `ADDITIONAL AGENTS: none currently running` and the new status.
4. Remove worker worktrees whose commits landed on the run branch (`git worktree remove`). Keep any with unlanded or failed work, and say which ones you kept.
5. Report to Yash: what shipped, the commit message, PR link (or the push command if Push/PR was `no`), checks, open blockers and where they were routed.
