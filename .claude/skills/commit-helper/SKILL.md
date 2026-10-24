---
name: commit-helper
description: Final step of a MeshThing run - parse .agent-work/changes.md into the pull request's commit message and description for the orchestrator's run branch, push/open the PR only if the run allows it, post a DONE message to Discord, and move the run's Kaneo tickets to in-review. Use after every worker has landed on the run branch and the checks pass.
argument-hint: "[run-branch]"
user-invocable: false
---

# Commit helper

Read `bot-protocol` first. Inputs: `$MAIN/.agent-work/changes.md`, `plan.md` (including its `Push/PR:` line), and the run branch (`$ARGUMENTS` or the branch named in `plan.md`, called `RUN` below).

The workers' commits are already on `RUN`. Don't rewrite, squash, or amend them. Your job is the message that describes the whole branch: the PR title and body, which GitHub also uses as the squash-merge commit message.

## 1. Parse the change log

Read `changes.md`. Group the lines by ticket and drop duplicates. For each SHA, check it's on the run branch (`git merge-base --is-ancestor <sha> "$RUN"`). If one isn't, a worker's work didn't land: stop and report it to the orchestrator.

Cross-check with `git log --oneline main.."$RUN"`. Mention any commit with no `changes.md` line (for example the orchestrator's check fixes) in one line under Changes.

## 2. Write the message

Write `$MAIN/.agent-work/commit-message.md`:

```markdown
<type>(<scope>): <imperative summary, ≤ 72 chars, covering the whole run>

<2-4 sentence overview: what the mesh can do now that it couldn't before, and why.>

Changes:
- M-<n> <ticket title>: <condensed from changes.md, one line per task>
- …

Checks: cargo build ✓, cargo test ✓, cargo clippy -D warnings ✓, cargo fmt --check ✓
Limitations: <from worker reports, or "none known">
Tickets: M-<n>, M-<m>

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>

---PR---
## Summary
<overview paragraph>

## Changes
<the same bullets, grouped by ticket, with the Kaneo task linked>

## Testing
<each check and how to reproduce the demo, e.g. two-terminal hello test>

## Known limitations / follow-ups
<items, with REFs of any blockers routed>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- The part above `---PR---` is the commit message: PR title = its first line, and it's the text to paste as the squash-merge message. The part below is the PR description.
- `type` is one of `feat`, `fix`, `refactor`, `test`, `docs`, `chore`. `scope` is the main area (`p2p`, `worker`, `history`).
- Only claim checks that were actually run and passed on the tip of `RUN`. Write failures as failures.
- Never put `.agent-work/` content other than this message, secrets, or local paths into the PR.

## 3. Push / PR (only if `Push/PR: yes` in plan.md)

- `git push -u origin "$RUN"`.
- If `gh` is installed: `gh pr create --base main --head "$RUN" --title "<first line>" --body-file <PR section>`. Otherwise give Yash the compare URL `https://github.com/yash-joshi-0/HackOHIO26/compare/main...<RUN>?expand=1` along with the title and body to paste.
- If `Push/PR: no`: don't push. Give Yash the exact push command, the title, and the body.

## 4. Announce

Post one `TYPE: DONE` message to #bot-coordination (`TO: all`, `TASK:` every ticket in the run). Body: the summary line, one line per ticket, the run branch and its tip SHA, the PR link (or "PR pending — branch is local, not pushed"), checks run, and limitations. Log it in `outbox.md`.

## 5. Move tickets

For each ticket in the run that's assigned to Yash (or that we claimed in this run): `update_task_status` → `in-review`, and add a Kaneo comment with the run branch, its tip SHA, and the PR link if there is one. Leave partly done tickets in `in-progress`, with a comment saying what's left. Never move tickets owned by others.

## 6. Blockers and missing features

For every unresolved item in the worker reports and `plan.md` (status `blocked`, or formatter LEFT items that need another owner), run `blocker-router` unless `outbox.md` shows it was already routed.

## 7. Return

The PR title and body (or the path to `commit-message.md`), the PR link or push command, the Discord message link, the tickets moved, and the blockers routed (REFs).
