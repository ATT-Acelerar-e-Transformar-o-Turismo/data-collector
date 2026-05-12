import logging
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class XValueType(str, Enum):
    DATETIME = "datetime"
    NUMBER = "number"
    STRING = "string"

class DataPoint(BaseModel):
    x: Union[str, float, int] = Field(..., description="X value: datetime string, number, or category string")
    y: float = Field(..., description="Numeric Y value")
    # Optional series label. Set by wrappers that emit multiple parallel
    # streams (e.g. one Excel file with N value columns → one DataPoint per
    # row × column tagged with the column name). When None, the point is
    # treated as belonging to the default/single series.
    series: Optional[str] = Field(default=None, description="Series label (column name for multi-column files)")

class WrapperMessage(BaseModel):
    wrapper_id: str = Field(..., description="UUID of the wrapper")
    data: List[DataPoint] = Field(..., description="Array of data points")
    metadata: Dict[str, Any] = Field(..., description="Source metadata")

    @field_validator('data')
    @classmethod
    def validate_no_duplicate_x(cls, v):
        # Collapse points sharing (x, series). Multi-series wrappers
        # legitimately emit one point per (x, column), so the dedup key
        # includes series. When the same (x, series) appears more than once
        # with conflicting y values — typically a wrapper that pulled hourly
        # rows under a day-only timestamp — we average the y's and log the
        # collision. Rejecting the whole message instead (the old behaviour)
        # left the user staring at an empty chart with nothing in the UI
        # explaining why; downstream merge dedups by (x, series) too, so
        # averaging here matches what eventually reaches the chart.
        if not v:
            return v

        from collections import OrderedDict

        grouped: "OrderedDict[tuple, list[float]]" = OrderedDict()
        sample_point: Dict[tuple, DataPoint] = {}
        for point in v:
            key = (point.x, point.series)
            if key not in grouped:
                grouped[key] = []
                sample_point[key] = point
            grouped[key].append(point.y)

        if len(grouped) == len(v):
            return v

        deduped: List[DataPoint] = []
        for key, ys in grouped.items():
            unique_ys = set(ys)
            base = sample_point[key]
            if len(unique_ys) <= 1:
                deduped.append(base)
                continue
            avg = sum(ys) / len(ys)
            logger.warning(
                "WrapperMessage: averaged %d conflicting y values for x=%r series=%r (values=%s, avg=%s)",
                len(ys), key[0], key[1], list(unique_ys), avg,
            )
            deduped.append(base.model_copy(update={"y": avg}))
        return deduped

class WrapperStatistics(BaseModel):
    wrapper_id: str
    last_message_timestamp: datetime
    total_messages: int
    x_value_type: XValueType
    last_data_count: int
    
class ValidationError(BaseModel):
    wrapper_id: str
    error_type: str
    error_message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    original_data: Dict[str, Any] = Field(default_factory=dict)