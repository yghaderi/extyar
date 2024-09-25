from pydantic import BaseModel

class Fee(BaseModel):
    short_call: float
    long_ua: float
    exercise: float

