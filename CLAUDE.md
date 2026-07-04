# scanrr

Scans a media library for corrupt video files using ffmpeg. **[SPEC.md](./SPEC.md)**
is the source of truth for design; **[IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)**
tracks the build.

## Conventions

### Enums, never bare strings for constrained values
Any value drawn from a fixed set (statuses, kinds, backends, algorithms, dispositions,
event types, …) MUST be a proper `enum.StrEnum` in `scanrr/enums.py` — never a bare
string literal or a class of `str` constants. This applies everywhere: function
params, dataclass fields, DB columns, API models.

- Define it in `scanrr/enums.py` as `class Foo(StrEnum): BAR = "bar"`.
- For SQLModel columns, map it with `scanrr.db.columns.enum_col(Foo)` so the stored
  value is the enum **value** (`"bar"`), not its name (`"BAR"`). SQLAlchemy's default
  Enum stores the name — which would silently break raw-SQL / partial-index filters
  that match on values.
- Compare with the enum (`x is Foo.BAR`), not string literals.

### Never `Any`, never type-ignores
Do not use `typing.Any` or `# type: ignore` / `# mypy: ignore` to silence the type
checker. Model the types properly instead — a typed dataclass/Pydantic model over a
heterogeneous dict, `sqlmodel.col(...)` for ORM column expressions, narrowing
`assert x is not None` after a flush, precise unions. If a type genuinely can't be
expressed, raise it rather than papering over it.

### Other
- Python 3.12, `ruff` + `mypy` clean before commit.
- Timestamps stored as ISO-8601 UTC strings via `scanrr.core.clock` (SPEC §8).
- Runtime tunables + their defaults live in `scanrr/core/config.py::DEFAULTS`
  (canonical per SPEC §13) — don't scatter magic numbers.
- Every schema change ships an Alembic migration (once Alembic lands in M2); never
  edit a released migration.
- When code and SPEC.md disagree, update the spec first, then the code.
