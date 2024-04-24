import pydantic
import typing


class AnalyticInfo(pydantic.BaseModel):
    category: str
    name: str
    type_id: int
    uid: str


class TechniqueInfo(pydantic.BaseModel):
    name: str
    uid: str


class AttackInfo(pydantic.BaseModel):
    tactic: TechniqueInfo
    technique: TechniqueInfo
    version: str


class FindingInfo(pydantic.BaseModel):
    analytic: AnalyticInfo
    attacks: AttackInfo
    title: str
    types: typing.List[str]
    uid: str


class ProductInfo(pydantic.BaseModel):
    name: str
    lang: str
    vendor_name: str


class Metadata(pydantic.BaseModel):
    log_name: str
    log_provider: str
    product: ProductInfo
    version: str


class Resource(pydantic.BaseModel):
    name: str
    uid: str


class DetectionFinding(pydantic.BaseModel):
    activity_id: int = 1
    category_name: str = "Findings"
    category_uid: int = 2
    class_name: str = "Detection Finding"
    class_uid: int = 2004
    count: int
    message: str
    finding_info: FindingInfo
    metadata: Metadata
    raw_data: str
    resources: typing.List[Resource]
    risk_score: int
    severity_id: int
    status_id: int = 99
    time: str
    type_uid: int = 200401
    unmapped: typing.Dict[str, typing.List[str]] = pydantic.Field()
