---
name: blocker-router
description: "Route a MeshThing blocker or missing feature to the right place - find the relevant Kaneo ticket; if someone else owns it, message their bot's inbox; otherwise post to #bot-coordination. Dedupes against .agent-work/outbox.md. Use whenever an agent is blocked by work outside its own scope or discovers a missing feature."
argument-hint: "<agent-name> <short blocker description>"
user-invocable: false
---

# Blocker router

Arguments: `$ARGUMENTS` — the reporting agent's name and a description of the blocker or missing feature. Read `bot-protocol` first.

## 1. Pin down the blocker

Write down, before searching:
- **What's blocked**: our ticket and task.
- **What's missing**: the feature, interface, decision, or fix needed. Be concrete: a type, function, endpoint, or behaviour.
- **What would unblock it**: the smallest deliverable, plus what we can do meanwhile.

## 2. Dedupe

Read `$MAIN/.agent-work/outbox.md` and the last ~30 messages in #bot-coordination. If the same blocker was already sent and isn't answered yet, don't send it again: add a line to `coordination.md` pointing at the existing REF and stop.

## 3. Find the relevant ticket

1. `search` (with `workspaceId`) for the key terms, and `list_tasks` for the MeshThing project. Match on title and description.
2. Choose the single best match. If two match, prefer the one `in-progress`.

## 4. Route

| Situation | Where it goes |
|---|---|
| Ticket assigned to **your human** (ours) | Not external. Append to `coordination.md` for the orchestrator to pick up as a new or follow-up task, and stop. |
| Ticket assigned to **someone else**, and their bot is in #bot-directory (match the directory's `HUMAN:` field to the Kaneo assignee) | `TYPE: BLOCKER` (or `REQUEST` for a missing feature) to **that bot's inbox**, `TO: <their bot-id>`, `TASK: M-<n>` of **their** ticket. |
| Ticket assigned to someone else, but **no bot** registered for that person | #bot-coordination, `TO: all`, naming the human and their ticket so they or any bot can pick it up. |
| **No matching ticket**, or the ticket is **unassigned** | #bot-coordination, `TO: all`. Propose the missing work as a new or unassigned task; don't create Kaneo tickets yourself unless your human asked for that. |

Message body, inside the protocol header (`REF: <bot-id>-<agent>-<nn>`, `REPLY-TO: #bot-inbox-<bot-id>`):

```text
Blocked: M-<ours> <title> (<bot-id>/<agent>)
Need: <concrete deliverable>
Why: <one line>
Meanwhile: <what we're doing instead, or "paused">
Response needed: <decision / ETA / interface signature>
```

Add a Kaneo comment on **their** ticket (`create_task_comment`) only when the message went to their inbox and the ticket is the obvious home for the request. Never change another person's ticket status, assignee, or description.

## 5. Record

1. Append to `$MAIN/.agent-work/outbox.md`: `- <REF> → <channel name> (<message link>) — <one-line summary>`.
2. Append to `coordination.md`: `[<time>] [<agent>] ROUTED <REF> to <channel>`.
3. Return the REF, the channel, and the message link.
