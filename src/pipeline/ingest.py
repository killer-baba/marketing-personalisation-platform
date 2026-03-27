from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator, ValidationError

from src.utils.logger import get_logger

logger = get_logger("pipeline.ingest")


# ── Pydantic schema ───────────────────────────────────────────

class ConversationRecord(BaseModel):
    user_id:     str  = Field(..., min_length=1, max_length=64)
    message:     str  = Field(..., min_length=1)
    campaign_id: str  = Field(..., min_length=1, max_length=64)
    intent:      str  = Field(..., min_length=1, max_length=128)
    channel:     str  = Field(default="web")
    timestamp:   str  = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # auto-generated fields (set during validation)
    message_id:  str  = Field(default_factory=lambda: str(uuid.uuid4()))
    lineage_id:  str  = Field(default_factory=lambda: str(uuid.uuid4()))

    @field_validator("message")
    @classmethod
    def message_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("message must not be blank or whitespace only")
        return v.strip()

    @field_validator("user_id", "campaign_id")
    @classmethod
    def no_whitespace(cls, v: str) -> str:
        if " " in v:
            raise ValueError("IDs must not contain spaces")
        return v


# ── Validation + lineage ──────────────────────────────────────

class IngestResult:
    """Holds the outcome of validating one raw record."""
    __slots__ = ("record", "error")

    def __init__(
        self,
        record: ConversationRecord | None = None,
        error:  str | None = None,
    ):
        self.record = record
        self.error  = error

    @property
    def ok(self) -> bool:
        return self.record is not None


def validate_record(raw: dict[str, Any]) -> IngestResult:
    """
    Validate a single raw dict against ConversationRecord.
    Returns IngestResult with either a valid record or an error string.
    """
    try:
        record = ConversationRecord(**raw)
        logger.debug(
            f"Valid record | user={record.user_id} "
            f"msg_id={record.message_id} lineage={record.lineage_id}"
        )
        return IngestResult(record=record)
    except ValidationError as exc:
        errors = "; ".join(
            f"{e['loc'][0]}: {e['msg']}" for e in exc.errors()
        )
        logger.warning(f"Validation failed | errors='{errors}' | raw={raw}")
        return IngestResult(error=errors)


def validate_batch(
    raw_records: list[dict[str, Any]],
) -> tuple[list[ConversationRecord], list[tuple[dict, str]]]:
    """
    Validate a batch of raw dicts.

    Returns:
        valid   — list of ConversationRecord
        invalid — list of (original_dict, error_string) for DLQ routing
    """
    valid:   list[ConversationRecord]       = []
    invalid: list[tuple[dict[str, Any], str]] = []

    for raw in raw_records:
        result = validate_record(raw)
        if result.ok:
            valid.append(result.record)
        else:
            invalid.append((raw, result.error))

    logger.info(
        f"Batch validation | total={len(raw_records)} "
        f"valid={len(valid)} invalid={len(invalid)}"
    )
    return valid, invalid