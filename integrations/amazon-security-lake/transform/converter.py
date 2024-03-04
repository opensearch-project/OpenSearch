import json

import pydantic
import transform.models as models


def normalize(level: int) -> int:
    """
    Normalizes rule level into the 0-6 range, required by OCSF.
    """
    if level >= 15:     # (5) Critical
        severity = 5
    elif level >= 11:   # (4) High
        severity = 4
    elif level >= 8:    # (3) Medium
        severity = 3
    elif level >= 4:    # (2) Low
        severity = 2
    elif level >= 0:    # (1) Informational
        severity = 1
    else:
        severity = 0    # (0) Unknown

    return severity


def join(iterable, separator=","):
    return (separator.join(iterable))


def to_detection_finding(event: models.wazuh.Event) -> models.ocsf.DetectionFinding:
    finding_info = models.ocsf.FindingInfo(
        analytic=models.ocsf.AnalyticInfo(
            category=", ".join(event.rule.groups),
            name=event.decoder.name,
            type_id=1,
            uid=event.rule.id
        ),
        attacks=models.ocsf.AttackInfo(
            tactic=models.ocsf.TechniqueInfo(
                name=", ".join(event.rule.mitre.tactic),
                uid=", ".join(event.rule.mitre.id)
            ),
            technique=models.ocsf.TechniqueInfo(
                name=", ".join(event.rule.mitre.technique),
                uid=", ".join(event.rule.mitre.id)
            ),
            version="v13.1"
        ),
        title=event.rule.description,
        types=[event.input.type],
        uid=event.id
    )

    metadata = models.ocsf.Metadata(
        log_name="Security events",
        log_provider="Wazuh",
        product=models.ocsf.ProductInfo(
            name="Wazuh",
            lang="en",
            vendor_name="Wazuh, Inc,."
        ),
        version="1.1.0"
    )

    resources = [models.ocsf.Resource(
        name=event.agent.name, uid=event.agent.id)]

    severity_id = normalize(event.rule.level)

    unmapped = {
        "data_sources": [
            event.location,
            event.manager.name
        ],
        "nist": event.rule.nist_800_53  # Array
    }

    return models.ocsf.DetectionFinding(
        count=event.rule.firedtimes,
        message=event.rule.description,
        finding_info=finding_info,
        metadata=metadata,
        raw_data=event.full_log,
        resources=resources,
        risk_score=event.rule.level,
        severity_id=severity_id,
        time=event.timestamp,
        unmapped=unmapped
    )


def from_json(event: dict) -> models.wazuh.Event:
    # Needs to a string, bytes or bytearray
    try:
        return models.wazuh.Event.model_validate_json(json.dumps(event))
    except pydantic.ValidationError as e:
        print(e)
