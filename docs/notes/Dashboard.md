---
type: dashboard
---

# Project Dashboard

> [!info] Requires the **Dataview** community plugin (Settings → Community plugins →
> Browse → "Dataview" → Install → Enable). Without it these blocks render as code
> fences. Everything else in this vault is plain Markdown and needs no plugin.

## Where the project stands

```dataview
TABLE WITHOUT ID
  status AS Status,
  length(rows) AS Packages,
  sum(rows.hours) AS Hours
FROM "docs/notes/tasks"
GROUP BY status
SORT status ASC
```

## Critical path — zero float

A day lost on any of these moves the end date.

```dataview
TABLE WITHOUT ID
  link(file.link, id) AS ID,
  title AS "Work package",
  week AS Week,
  hours AS h,
  status AS Status
FROM "docs/notes/tasks"
WHERE critical_path = true
SORT id ASC
```

## This week's work

Change the `week_num` filter as the project moves.

```dataview
TABLE WITHOUT ID
  link(file.link, id) AS ID,
  title AS "Work package",
  hours AS h,
  status AS Status,
  join(depends_on, ", ") AS "Depends on"
FROM "docs/notes/tasks"
WHERE week_num = 1 AND status != "done"
SORT id ASC
```

## Everything still open, by week

```dataview
TABLE WITHOUT ID
  link(file.link, id) AS ID,
  title AS "Work package",
  hours AS h,
  status AS Status
FROM "docs/notes/tasks"
WHERE status != "done"
GROUP BY week_num AS Week
SORT week_num ASC
```

## Effort roll-up by WBS element

```dataview
TABLE WITHOUT ID
  element AS Element,
  sum(rows.hours) AS "Planned h",
  sum(filter(rows, (r) => r.status = "done").hours) AS "Done h",
  length(rows) AS Packages
FROM "docs/notes/tasks"
GROUP BY element
SORT element ASC
```

## Blocked packages

Every `todo` package whose dependencies are not all `done`. Needs **JavaScript queries**
enabled in Dataview settings.

```dataviewjs
const tasks = dv.pages('"docs/notes/tasks"').where(p => p.type == "task");
const byId = new Map(tasks.map(p => [String(p.id), p]));
const rows = [];
for (const t of tasks.where(p => p.status == "todo")) {
  const waiting = (t.depends_on ?? [])
    .map(String)
    .filter(d => byId.has(d) && byId.get(d).status != "done");
  if (waiting.length) {
    rows.push([
      dv.fileLink(t.file.path, false, t.id),
      t.title,
      t.week,
      waiting.map(d => dv.fileLink(byId.get(d).file.path, false, d)),
    ]);
  }
}
rows.sort((a, b) => String(a[1]).localeCompare(String(b[1])));
dv.table(["ID", "Work package", "Week", "Waiting on"], rows);
```

## Ready to start

Nothing in the way — dependencies all `done` (or none).

```dataviewjs
const tasks = dv.pages('"docs/notes/tasks"').where(p => p.type == "task");
const byId = new Map(tasks.map(p => [String(p.id), p]));
const rows = [];
for (const t of tasks.where(p => p.status == "todo")) {
  const waiting = (t.depends_on ?? [])
    .map(String)
    .filter(d => byId.has(d) && byId.get(d).status != "done");
  if (!waiting.length) {
    rows.push([
      dv.fileLink(t.file.path, false, t.id),
      t.title,
      t.week,
      t.hours,
      t.critical_path ? "yes" : "",
    ]);
  }
}
dv.table(["ID", "Work package", "Week", "h", "Critical path"], rows);
```

## Open risks by score

```dataview
TABLE WITHOUT ID
  link(file.link, id) AS ID,
  title AS Risk,
  score AS Score,
  band AS Band,
  reviewed AS "Last reviewed"
FROM "docs/notes/risks"
WHERE open = true
SORT score DESC
```

## Risks not reviewed in the last 7 days

The register is meant to be reviewed every Monday — WBS [1.2.1](tasks/WBS-1.2.1.md).

```dataview
TABLE WITHOUT ID
  link(file.link, id) AS ID,
  title AS Risk,
  reviewed AS "Last reviewed"
FROM "docs/notes/risks"
WHERE reviewed < date(today) - dur(7 days)
SORT reviewed ASC
```

## Sources

```dataview
TABLE WITHOUT ID
  file.link AS Source,
  domain AS Domain,
  needs_js AS "JS?",
  robots_checked AS "robots.txt checked",
  status AS Status
FROM "docs/notes/sources"
SORT file.name ASC
```

## Recent daily notes

```dataview
TABLE WITHOUT ID
  file.link AS Day,
  hours AS h,
  cost_usd AS "USD",
  join(tags, ", ") AS Tags
FROM "docs/notes/daily"
SORT file.name DESC
LIMIT 10
```

## Anything blocked, anywhere

```dataview
LIST
FROM #blocked
SORT file.name DESC
```

## Open checkboxes across all daily notes

```dataview
TASK
FROM "docs/notes/daily"
WHERE !completed
SORT file.name DESC
```
