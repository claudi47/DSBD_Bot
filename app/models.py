import datetime
from typing import ClassVar, Optional, List

from bson import ObjectId
from pydantic import BaseModel, Field


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


def get_timezoneaware_now():
    return datetime.datetime.now(datetime.timezone.utc)


class User(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias='_id')
    username: str
    user_id: str
    max_research: Optional[int] = ...
    ban_period: Optional[datetime.datetime] = ...
    timestamp: datetime.datetime = Field(default_factory=get_timezoneaware_now)


class UserAuthTransfer(BaseModel):
    username: str
    user_id: str


class UserAuthTransferReply(UserAuthTransfer):
    authorized: bool


class SearchDataInDb(BaseModel):
    collection_name: ClassVar[str] = 'search_history'
    id: PyObjectId = Field(default_factory=PyObjectId, alias='_id')
    csv_url: str
    web_site: str
    timestamp: datetime.datetime = Field(default_factory=get_timezoneaware_now)
    user_id: str

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class SearchDataPartialInDb(BaseModel):
    collection_name: ClassVar[str] = SearchDataInDb.collection_name

    id: PyObjectId = Field(default_factory=PyObjectId, alias='_id')
    web_site: str
    user_id: str

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class BetDataInDb(BaseModel):
    collection_name: ClassVar[str] = 'bet_data'
    id: PyObjectId = Field(default_factory=PyObjectId, alias='_id')
    date: str
    match: str
    one: str
    ics: str
    two: str
    gol: str
    over: str
    under: str
    timestamp: datetime.datetime = Field(default_factory=get_timezoneaware_now)
    search_id: PyObjectId  # reference to Search document in the Search history collection

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class BetDataPartialInDb(BaseModel):
    collection_name: ClassVar[str] = BetDataInDb.collection_name
    id: PyObjectId = Field(default_factory=PyObjectId, alias='_id')
    search_id: PyObjectId

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class BetDataUpdateInDb(BaseModel):
    collection_name: ClassVar[str] = BetDataInDb.collection_name
    date: str
    match: str
    one: str
    ics: str
    two: str
    gol: str
    over: str
    under: str
    timestamp: datetime.datetime = Field(default_factory=get_timezoneaware_now)


class BetDataUpdateList(BaseModel):
    data: List[BetDataUpdateInDb]
