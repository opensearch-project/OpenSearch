import pydantic
import typing
import abc


class AnalyticInfo(pydantic.BaseModel):
    category: str
    name: str
    type_id: int = 1
    uid: str


# Deprecated since v1.1.0. Use AnalyticInfo instead.
class Analytic(pydantic.BaseModel):
    category: str
    name: str
    type: str = "Rule"
    type_id: int = 1
    uid: str


class TechniqueInfo(pydantic.BaseModel):
    name: str
    uid: str


class AttackInfo(pydantic.BaseModel):
    tactic: TechniqueInfo
    technique: TechniqueInfo
    version: str = "v13.1"


class FindingInfo(pydantic.BaseModel):
    analytic: AnalyticInfo
    attacks: typing.List[AttackInfo]
    title: str
    types: typing.List[str]
    uid: str


# Deprecated since v1.1.0. Use FindingInfo instead.
class Finding(pydantic.BaseModel):
    title: str
    types: typing.List[str]
    uid: str


class ProductInfo(pydantic.BaseModel):
    name: str
    lang: str
    vendor_name: str


class Metadata(pydantic.BaseModel):
    log_name: str = "Security events"
    log_provider: str = "Wazuh"
    product: ProductInfo = ProductInfo(
        name="Wazuh",
        lang="en",
        vendor_name="Wazuh, Inc,."
    )
    version: str = "1.1.0"


class Resource(pydantic.BaseModel):
    name: str
    uid: str


class FindingABC(pydantic.BaseModel, abc.ABC):
    activity_id: int = 1
    category_name: str = "Findings"
    category_uid: int = 2
    class_name: str
    class_uid: int
    count: int
    message: str
    metadata: Metadata = Metadata()
    raw_data: str
    resources: typing.List[Resource]
    risk_score: int
    severity_id: int
    status_id: int = 99
    time: int
    type_uid: int
    unmapped: typing.Dict[str, typing.List[str]] = pydantic.Field()


class DetectionFinding(FindingABC):
    class_name: str = "Detection Finding"
    class_uid: int = 2004
    finding_info: FindingInfo
    type_uid: int = 200401


# Deprecated since v1.1.0. Use DetectionFinding instead.
class SecurityFinding(FindingABC):
    analytic: Analytic
    attacks: typing.List[AttackInfo]
    class_name: str = "Security Finding"
    class_uid: int = 2001
    finding: Finding
    state_id: int = 1
    type_uid: int = 200101
