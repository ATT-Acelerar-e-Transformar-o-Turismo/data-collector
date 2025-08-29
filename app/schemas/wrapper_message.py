from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any, Union
from datetime import datetime
from enum import Enum

class XValueType(str, Enum):
    DATETIME = "datetime"
    NUMBER = "number"
    STRING = "string"

class DataPoint(BaseModel):
    x: Union[str, float, int] = Field(..., description="X value: datetime string, number, or category string")
    y: float = Field(..., description="Numeric Y value")

class WrapperMessage(BaseModel):
    wrapper_id: str = Field(..., description="UUID of the wrapper")
    data: List[DataPoint] = Field(..., description="Array of data points")
    metadata: Dict[str, Any] = Field(..., description="Source metadata")
    
    @field_validator('data')
    @classmethod
    def validate_no_duplicate_x(cls, v):
        if not v:
            return v
            
        x_values = [point.x for point in v]
        duplicates = []
        seen = set()
        
        for i, x in enumerate(x_values):
            if x in seen:
                duplicates.append(i)
            else:
                seen.add(x)
        
        if duplicates:
            # Check for conflicting y values at same x
            x_to_points = {}
            for point in v:
                if point.x not in x_to_points:
                    x_to_points[point.x] = []
                x_to_points[point.x].append(point.y)
            
            for x, y_values in x_to_points.items():
                if len(set(y_values)) > 1:
                    raise ValueError(f"Conflicting y values {list(set(y_values))} for x={x}")
            
            # Remove duplicates, keeping first occurrence
            unique_points = []
            seen_x = set()
            for point in v:
                if point.x not in seen_x:
                    unique_points.append(point)
                    seen_x.add(point.x)
            return unique_points
        
        return v

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