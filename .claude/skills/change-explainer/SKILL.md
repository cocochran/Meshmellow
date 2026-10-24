---
name: change-explainer
description: Append a 1-2 sentence, high-level explanation of a finished MeshThing task to the gitignored .agent-work/changes.md, which commit-helper later turns into the PR commit message. Use after a worker's change is formatted and landed on the run branch.
argument-hint: "<agent-name> <ticket e.g. M-4> <run-branch> <commit-sha>"
user-invocable: false
---

# Change explainer

Arguments: `$ARGUMENTS` — agent name, ticket, run branch, and the commit SHA as landed on the run branch (if there are several, the range `<first>^..<last>`).

## 1. Read the change

```bash
MAIN="$(git worktree list --porcelain | head -1 | cut -d' ' -f2-)"
git show --stat <sha>
git show <sha>
```

Read the diff, not just the commit message. If the commit is large, read `--stat` plus the hunks that define new types, functions, or behaviour.

## 2. Write the explanation

1–2 sentences, at most about 50 words, for a teammate who hasn't seen the code:

- Say **what capability changed and why it matters** to the mesh (e.g. "Peers can now open an Iroh connection by node ID and exchange a hello message, proving basic connectivity").
- Name the main module or type once if it helps someone find it. No line-by-line narration, no file lists, no hype.
- Mention a limitation only if it changes what someone can rely on ("LAN only; NAT traversal untested").
- Use plain present tense. No first person, no "this commit".

## 3. Append

Write exactly one line with a single append so parallel agents don't interleave:

```bash
mkdir -p "$MAIN/.agent-work"
printf '%s\n' "- **M-4** · \`p2p-endpoint\` · \`<bot-id>/run-20261024-1530\` @ \`abc1234\` — <your 1-2 sentences>" >> "$MAIN/.agent-work/changes.md"
```

If the file is new, first append the header `# Change log for this run` (check with `test -s`). Never rewrite or reorder existing lines. Make sure `.agent-work/` is listed in `$MAIN/.gitignore` — if not, report that instead of adding the file to git.

## 4. Return

Return the exact line you appended.
