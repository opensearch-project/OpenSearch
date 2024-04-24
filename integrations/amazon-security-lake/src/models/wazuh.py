import pydantic
import typing

# =========== Wazuh event models =========== #
# These are only the fields required for the integration.


class Mitre(pydantic.BaseModel):
    technique: typing.List[str] = ["N/A"]
    id: typing.List[str] = ["N/A"]
    tactic: typing.List[str] = ["N/A"]


class Rule(pydantic.BaseModel):
    firedtimes: int = 0
    description: str = "N/A"
    groups: typing.List[str] = []
    id: str = "N/A"
    mitre: Mitre = Mitre()
    level: int = 0
    nist_800_53: typing.List[str] = []


class Decoder(pydantic.BaseModel):
    name: str = "N/A"


class Input(pydantic.BaseModel):
    type: str = "N/A"


class Agent(pydantic.BaseModel):
    name: str
    id: str


class Manager(pydantic.BaseModel):
    name: str


class Event(pydantic.BaseModel):
    rule: Rule = Rule()
    decoder: Decoder = Decoder()
    input: Input = Input()
    id: str = ""
    full_log: str = ""
    agent: Agent = {}
    timestamp: str = ""
    location: str = ""
    manager: Manager = {}
