# Documentation conventions

## No decorative icons

Documents do not use emoji or icons (locks, check marks, warning signs, coloured dots, and so on): they render differently across operating systems and fonts, which hurts readability instead of helping it.

Say everything in words:

- States and results use words: yes / no, supported / unsupported, done / in progress / not started, healthy / failing, high / medium / low.
- A status column spells the state out rather than using a check, a cross or a coloured dot; when a legend is needed, write the legend in words.
- Notes and warnings use a text label ("Note:", "Important:", "Implemented:") instead of an emoji prefix.
- Allowed, because they render consistently in a monospace font and are structure rather than decoration: ASCII box drawing (┌─┘├), geometric arrows (▲ ► ▼), circled step numbers (① ② ③), plain-text arrows (→ ← ↓), and keyboard shortcuts written out (such as Cmd+K).

## No em dashes

Never use an em dash or an en dash in prose, in either language.

- Introduce an explanation with a colon, separate clauses with a comma or a semicolon, and put an aside in parentheses.
- Hyphens in code, flags, file names, and compound words are unaffected.
