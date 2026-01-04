from pydantic import BaseModel
from typing import List
from fastapi import UploadFile

class RaceUDPs(BaseModel):
    file: bytes

class RaceRequest(BaseModel):
    data: str  # Base64 encoded string