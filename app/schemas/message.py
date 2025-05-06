from pydantic import BaseModel
from typing import List

class DataPoint(BaseModel):
    x: str
    y: float

class DataMessage(BaseModel):
    wrapper_id: int
    data_segment: List[DataPoint] 