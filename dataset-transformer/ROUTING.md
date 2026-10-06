# How the pipeline decides: local vs. LLM (6G-DALI mode)

The pipeline turns a dataset into a **6G-DALI-compliant CSV**. Per run it asks:
**"is this data already DALI-clean tabular, or does it need a meaning-changing
transform?"** The decision is a structural check, not a guess. The logic lives in
`profile()` and `main()` in [pipeline.py](pipeline.py).

## The check — `profile()`

It scans the records (up to `_SCAN_LIMIT`) and works out four things that force a
transform, plus the hints the LLM needs:

```python
needs_llm = bool(nested_fields or non_dict or long_format or unit_fields)
```

A dataset goes to the **LLM** if **any** of these is true:

1. **Nested fields** — a value is a JSON object or array (needs flattening / splitting).
2. **Non-object records** — a record isn't a JSON object at all.
3. **Long-format** — a timestamp column exists (matched against the §6.2 name list)
   **and its values repeat**, i.e. a series-identifier column is stacking several
   time series in one file. This needs a long→wide **pivot** (§7.2 / §7.3).
4. **Embedded units** — a column whose values look like `number+unit` (`2048M`,
   `12ms`, `85%`), which must be split into a numeric column (§5.2 / §7.1).

If none of those hold (and you didn't pass `--force-llm`), the data is
**DALI-clean** and stays local.

## The routing — `main()`

```python
if not p["needs_llm"] and not args.force_llm:
    local_transform(...)   # DALI-clean: snake_case, empty nulls, sort — no AWS
else:
    generate_code(...)     # Bedrock writes a bespoke DALI transform
```

**Local tier** (deterministic, free, no credentials) applies the mechanical DALI
rules: snake_case column names + collision detection (§4.2), empty-cell nulls
(§5.1), ascending timestamp sort (§5.3), UTF-8 / no BOM (§3.1).

**LLM tier** (Bedrock, behind the save→preview→confirm gate) handles the
meaning-changing rules via the prompt contract: flatten nested objects, split
embedded units and composite cells, pivot long→wide, no sentinel padding. This
matches the spec's rule that such steps are "a proposed transformation you
confirm", never applied silently.

`--force-llm` skips the check and sends everything to the LLM. `--script PATH`
reuses a saved script with no analysis or model call.

## Why a check, and not "try local, fall back on error"

The local transform **never crashes** on data that needs work — it would just
emit a wrong-but-valid CSV (a stringified nested object, a stacked long-format
table, a unit baked into a text column). There is no exception to catch, so the
router must inspect the structure **up front** and decide before running anything.

## Worked examples (the tested files)

| File | What `profile()` finds | Decision |
|------|------------------------|----------|
| `countries.json` | flat scalars, no timestamp | **local** (snake_case) |
| `airports.json` | flat scalars | **local** |
| `continents.json` | flat scalars (`null` is scalar) | **local** |
| `colors.json` | nested `rgb` object | **LLM** — `rgb` → `rgb_r/g/b` |
| `config-eslint.json` | nested `env`, `rules`, … | **LLM** — flatten config tree |
| `data/1.jsonl` (telemetry) | `Timestamp` repeats, stacked by `Node_ID`/`Variable` | **LLM** — long→wide pivot |

The telemetry row is the important one: nothing about it is *nested* (the CSI hex
is a single long **string**), but its **timestamps repeat** — so it's long-format
and routes to the pivot. The tool routes on the data's **structure and
DALI-readiness**, not on how messy a string looks inside.
