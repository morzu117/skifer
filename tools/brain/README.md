# Local Brain Notes

This repository uses `.gstack/`, `features/`, `adr/`, and `init.md` as the
versioned source material for project memory.

Preferred retrieval tool on this machine is `gbrain` when available:

```bash
gbrain search "<terms>"
gbrain code-def <symbol>   # once a code-aware sync pack is active
gbrain code-refs <symbol>  # once a code-aware sync pack is active
```

Runtime indices and caches are local-only and must not be committed:

- `.brain-runtime/`
- `.pip-cache/`
- `.gbrain-source`

Use retrieval as advisory context only. Repository files, ADRs, roadmap plans,
and explicit human decisions remain the source of truth.
