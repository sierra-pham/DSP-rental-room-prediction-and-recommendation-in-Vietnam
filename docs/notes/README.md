# Working notes vault

This repository doubles as an [Obsidian](https://obsidian.md/) vault. Obsidian edits the
Markdown files that are already here — it adds linking, backlinks, search and live tables
on top of them, and stores nothing of its own beyond the `.obsidian/` settings folder.
Nothing here requires Obsidian: every file is plain Markdown that renders on GitHub.

## Opening it

1. Install Obsidian, choose **Open folder as vault**, and pick the repository root
   (`DSP/`), not `docs/`. The committed `.obsidian/app.json` already excludes `.venv/`,
   `.git/` and the build directories from indexing.
2. Install one community plugin: **Settings → Community plugins → Browse → Dataview →
   Install → Enable**. Then in its settings, turn on **Enable JavaScript queries** (two
   blocks in the dashboard need it). Obsidian does not install plugins from a committed
   file — `.obsidian/community-plugins.json` only records which one this vault expects.
3. Open [Dashboard](Dashboard.md).

## Layout

| Path | What lives there |
|---|---|
| [Dashboard.md](Dashboard.md) | Live tables: critical path, this week's work, blocked packages, open risks, sources, recent days |
| [tasks/](tasks/) | One note per WBS work package (64), generated from [07-work-breakdown-structure.md](../07-work-breakdown-structure.md) |
| [risks/](risks/) | One note per risk (13), generated from [05-risk-register.md](../05-risk-register.md) |
| [sources/](sources/) | One note per portal — robots.txt verdicts, selectors, quirks, breakage history |
| [daily/](daily/) | Dated run log: what was done, the day's numbers, what broke |
| [decisions/](decisions/) | Short decision records — the constraint that forced a choice, and what it costs later |
| `_templates/` | Templates for the four note types |
| `assets/` | Images pasted into notes |

## The split that matters

The numbered documents `01`–`07` in [docs/](../) are **deliverables**. They stay plain,
GitHub-readable Markdown — supervisors and graders read them on GitHub, where Obsidian's
`[[wikilink]]` syntax and Dataview tables do not render.

This `notes/` folder is the **working layer**: many small notes, heavily cross-linked,
where finding things by relationship beats finding them by filename. Dataview tables here
render only inside Obsidian; on GitHub they appear as code blocks. That is fine — they
are a personal tool, not a deliverable.

Because of that split, the vault is configured to write **standard Markdown links**
(`[text](path.md)`), not wikilinks, so links keep working in both places.

## Conventions

- **Daily note:** `Ctrl+P → Daily note` (or the calendar ribbon). Fill the numbers table
  even on quiet days — Report 4 cites cost and volume evidence, and WBS
  [1.2.3](tasks/WBS-1.2.3.md) requires six dated entries.
- **Task status:** edit `status` in a task note's frontmatter — `todo` → `in-progress` →
  `done`. The dashboard rolls these up; nothing else needs updating.
- **Blocked?** Tag `#blocked` in the daily note and link the risk. It surfaces on the
  dashboard.
- **Risk review:** every Monday, update `reviewed:` (and `score:`/`status:` if they
  changed) in each risk note. Risks untouched for 7 days show up on the dashboard.
- **Decisions:** when a choice closes off an alternative, write a decision note. The
  "design changes explained" section of Report 4 is much easier to write from these than
  from memory.

## Regenerating

`tasks/` and `risks/` were generated once from the WBS and risk register. They are
editable notes now, not build output — do not regenerate over them. If the WBS gains a
work package, add the note by hand from `_templates/task.md`, and keep the WBS document
as the source of truth for scope.

## What is not committed

`.obsidian/workspace.json` (which panes you had open), the plugin code itself, and the
local file-recovery cache are gitignored. Settings, hotkeys and the daily-note/template
configuration are committed so the vault behaves the same on any machine.
