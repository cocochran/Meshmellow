# MeshThing agent skills

Claude Code skills for running parallel agent work on MeshThing. They work for any teammate on Windows, macOS or Linux, and each person runs them as their own bot. Start a run with `/mesh-orchestrator [ticket numbers]`.

## One-time setup (each person)

1. In Claude Code, connect the **Kaneo** and **Discord** MCP servers (`/mcp` should show both as connected).
2. Copy `.claude/bot-identity.example.json` to `.claude/bot-identity.json` (gitignored) and fill it in:
   - `botId`: an unused ID such as `yourname-claude` (check #bot-directory).
   - `human`: your name.
   - `kaneoUserId`: from Kaneo's `whoami`.
   - `inboxChannelId` and `categoryId`: create the category `Bot • <botId>` with a `bot-inbox-<botId>` channel inside it.

   Claude can do steps 2–3 for you: ask it to "set up my bot identity for the mesh skills".
3. Introduce your bot in #bot-directory.
4. On Windows, run Claude Code with Git Bash available. The skills' shell snippets are POSIX `sh` (macOS and Linux shells work as is).

Without `bot-identity.json`, the skills stop and ask you to set it up rather than borrowing someone else's identity.

```
bot-protocol                 shared IDs, message format, .agent-work/ layout (loaded by all)
└─ mesh-orchestrator         plans, spawns workers, polls inbox, tracks plan.md
   ├─ bot-inbox-check        inbox + #bot-coordination → ACKs, plan.md, Kaneo   (cron, ~10 min)
   ├─ mesh-task-worker  ×N   one per task, parallel, each in its own git worktree
   │  ├─ clean-code-formatter   format + lint + clean-code rules on changed files
   │  ├─ change-explainer       1-2 sentence summary → .agent-work/changes.md
   │  └─ blocker-router         ticket owner's inbox, else #bot-coordination
   └─ commit-helper          changes.md → PR commit message → Discord DONE → Kaneo in-review
      └─ blocker-router
```

## Flow

1. **Orchestrator** reads the inbox and the Kaneo board, splits tickets into tasks with separate file ownership, gets your human's approval (including whether to push/open a PR), creates the run branch `<bot-id>/run-<stamp>`, and spawns workers in parallel.
2. **Workers** implement their task in their own worktrees and coordinate through `.agent-work/coordination.md`. Each runs the **formatter**, commits **into the shared run branch** (rebase onto its tip, then a compare-and-swap ref update, retrying if another worker landed first), runs the **explainer**, and reports.
3. While workers run, the orchestrator re-runs **bot-inbox-check** on a cron and after each worker report, relaying answers to workers and updating `plan.md` and Kaneo.
4. **Blockers** go through **blocker-router**: to the inbox of whoever owns the relevant ticket, or to #bot-coordination if no one does.
5. Once every task has landed on the run branch and checks pass, **commit-helper** writes the PR commit message from `changes.md` (worker commits stay as they are), pushes and opens a PR only if allowed, posts DONE to Discord, and moves the tickets to `in-review`.

Run state lives in the gitignored `.agent-work/` folder at the main worktree root.

Workers run in fresh git worktrees, which only contain committed files, so commit `.claude/skills/` before starting a run. Workers fall back to reading the skill files from the main worktree path if a skill isn't loaded.
