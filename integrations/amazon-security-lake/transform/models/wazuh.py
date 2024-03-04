import pydantic
import typing

# =========== Wazuh event models =========== #
# These are only the fields required for the integration.


class Mitre(pydantic.BaseModel):
    technique: typing.List[str] = []
    id: typing.List[str] = ""
    tactic: typing.List[str] = []


class Rule(pydantic.BaseModel):
    firedtimes: int = 0
    description: str = ""
    groups: typing.List[str] = []
    id: str = ""
    mitre: Mitre = Mitre()
    level: int = 0
    nist_800_53: typing.List[str] = []


class Decoder(pydantic.BaseModel):
    name: str


class Input(pydantic.BaseModel):
    type: str


class Agent(pydantic.BaseModel):
    name: str
    id: str


class Manager(pydantic.BaseModel):
    name: str


class Event(pydantic.BaseModel):
    rule: Rule = {}
    decoder: Decoder = {}
    input: Input = {}
    id: str = ""
    full_log: str = ""
    agent: Agent = {}
    timestamp: str = ""
    location: str = ""
    manager: Manager = {}
