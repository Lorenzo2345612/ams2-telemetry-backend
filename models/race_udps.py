from pydantic import BaseModel
from typing import List, Optional
from fastapi import UploadFile

class RaceUDPs(BaseModel):
    file: bytes

class RaceRequest(BaseModel):
    data: str  # Base64 encoded string
    vehicle_name: Optional[str] = "Unknown"
    class_name: Optional[str] = "Unknown"