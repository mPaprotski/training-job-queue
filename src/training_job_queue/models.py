"""Validated HTTP request and response contracts."""

from datetime import datetime
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictBool, StrictInt, field_validator


class Simulation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    duration_seconds: Annotated[float, Field(strict=True, ge=0, le=300, allow_inf_nan=False)] | None = None
    fail_until_attempt: Annotated[StrictInt, Field(ge=0, le=1000)] = 0
    permanent_failure: StrictBool = False


class CreateJob(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payload: dict[str, JsonValue]

    @field_validator("payload")
    @classmethod
    def finite_json(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        try:
            encoded = json.dumps(value, allow_nan=False, ensure_ascii=False)
            encoded.encode("utf-8")
        except (ValueError, UnicodeError, RecursionError) as error:
            raise ValueError("Payload must contain finite, valid Unicode JSON values") from error
        # PostgreSQL JSONB cannot represent the NUL character, including in keys.
        if "\\u0000" in json.dumps(value):
            # Inspect actual strings so a literal backslash-u0000 remains valid.
            def contains_nul(item: JsonValue) -> bool:
                if isinstance(item, str):
                    return "\x00" in item
                if isinstance(item, dict):
                    return any("\x00" in key or contains_nul(child) for key, child in item.items())
                if isinstance(item, list):
                    return any(contains_nul(child) for child in item)
                return False

            if contains_nul(value):
                raise ValueError("Payload cannot contain NUL characters")
        if "simulation" in value:
            Simulation.model_validate(value["simulation"])
        return value


class Job(BaseModel):
    id: UUID
    idempotency_key: str
    payload: dict[str, JsonValue]
    state: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    attempt_no: int
    max_attempts: int
    worker_id: str | None
    lease_expires_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    result: JsonValue = None
    error: str | None


class JobPage(BaseModel):
    items: list[Job]
    next_cursor: str | None
