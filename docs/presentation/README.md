# MeshThing presentation

Open `index.html` in a browser. The nine-slide deck runs offline with no build,
fonts, libraries, or external assets. Source links need network access.

- Arrow keys, Space or Page Down: next slide; Page Up: previous.
- Home / End: first / last slide. Slide links use `#1` through `#9`.
- N or **Speaker notes**: show presenter notes on the current slide.
- **Print / PDF**: print all slides in landscape; notes and controls are omitted.
- Without JavaScript, all slides remain readable in one scrolling document.

The talk is approximately five minutes. Notes include timing, technical detail,
and caveats. For a short demo, use the slide 7 commands from the repository root.
Do not connect to the synthetic fixture's example worker identity.

This is a dated snapshot prepared October 24, 2026, against main `094ad51`.
Measured results are attributed to David's model-validation branch at `f6f682b`;
they are not new presentation-author measurements. Launcher PR10 was in review
when prepared. Refresh those statuses and demo commands before a live talk.

## Before presenting

1. Run the local fixture and confirm it passes. If using a real inference demo,
   coordinate the runtime and worker with David/Yash; keep the fixture as backup.
2. Check that the latest source and PR statuses agree with slide 8.
3. Preview the deck on the presentation display and check print preview if
   exporting PDF. Reduce browser zoom if the display is unusually short.
4. Keep limitations explicit: no demonstrated larger-than-one-device capacity,
   trustworthy peak-memory acceptance, cap enforcement or repeatable speedup.

Scope: presentation artifact and rehearsal guidance only. It does not provision
devices, start a runtime or publish a website.
