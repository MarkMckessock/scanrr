"""SQLModel column helpers."""

from __future__ import annotations

from enum import Enum

from sqlalchemy import Column
from sqlalchemy import Enum as SAEnum


def enum_col(enum_cls: type[Enum], *, nullable: bool = False) -> Column:
    """A column that stores an enum's **value** (not its name) as TEXT.

    SQLAlchemy's default Enum stores ``member.name`` (e.g. ``"PENDING"``); we want
    the spec value (``"pending"``) so raw SQL / partial indexes match. Build a
    fresh Column per field (Columns can't be shared).
    """
    return Column(
        SAEnum(
            enum_cls,
            native_enum=False,
            values_callable=lambda e: [m.value for m in e],
        ),
        nullable=nullable,
    )
