import pydantic
import models
import logging
from datetime import datetime


timestamp_pattern = "%Y-%m-%dT%H:%M:%S.%f%z"


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
    """
    Convert Wazuh security event to OCSF detection finding.
    """
    try:

        finding_info = models.ocsf.FindingInfo(
            analytic=models.ocsf.AnalyticInfo(
                category=", ".join(event.rule.groups),
                name=event.decoder.name,
                uid=event.rule.id
            ),
            attacks=[
                models.ocsf.AttackInfo(
                    tactic=models.ocsf.TechniqueInfo(
                        name=", ".join(event.rule.mitre.tactic),
                        uid=", ".join(event.rule.mitre.id)
                    ),
                    technique=models.ocsf.TechniqueInfo(
                        name=", ".join(event.rule.mitre.technique),
                        uid=", ".join(event.rule.mitre.id)
                    )
                )
            ],
            title=event.rule.description,
            types=[event.input.type],
            uid=event.id
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
            raw_data=event.full_log,
            resources=resources,
            risk_score=event.rule.level,
            severity_id=severity_id,
            time=to_epoch(event.timestamp),
            unmapped=unmapped
        )
    except AttributeError as e:
        logging.error(f"Error transforming event: {e}")
        return {}


def to_security_finding(event: models.wazuh.Event) -> models.ocsf.SecurityFinding:
    """
    Convert Wazuh security event to OCSF's Security Finding class.
    """
    try:

        analytic = models.ocsf.Analytic(
            category=", ".join(event.rule.groups),
            name=event.decoder.name,
            uid=event.rule.id
        )

        attacks = [
            models.ocsf.AttackInfo(
                tactic=models.ocsf.TechniqueInfo(
                    name=", ".join(event.rule.mitre.tactic),
                    uid=", ".join(event.rule.mitre.id)
                ),
                technique=models.ocsf.TechniqueInfo(
                    name=", ".join(event.rule.mitre.technique),
                    uid=", ".join(event.rule.mitre.id)
                )
            )
        ]

        finding = models.ocsf.Finding(
            title=event.rule.description,
            types=[event.input.type],
            uid=event.id
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

        return models.ocsf.SecurityFinding(
            analytic=analytic,
            attacks=attacks,
            count=event.rule.firedtimes,
            message=event.rule.description,
            finding=finding,
            raw_data=event.full_log,
            resources=resources,
            risk_score=event.rule.level,
            severity_id=severity_id,
            time=to_epoch(event.timestamp),
            unmapped=unmapped
        )
    except AttributeError as e:
        logging.error(f"Error transforming event: {e}")
        return {}


def to_epoch(timestamp: str) -> int:
    return int(datetime.strptime(timestamp, timestamp_pattern).timestamp())


def from_json(json_line: str) -> models.wazuh.Event:
    """
    Parse the JSON string representation of a Wazuh security event into a dictionary (model).
    """
    # Needs to a string, bytes or bytearray
    try:
        return models.wazuh.Event.model_validate_json(json_line)
    except pydantic.ValidationError as e:
        print(e)


def transform_events(events: list, ocsf_class: str) -> list:
    """
    Transform a list of Wazuh security events (json string) to OCSF format.
    """
    logging.info("Transforming Wazuh security events to OCSF.")
    ocsf_events = []
    for event in events:
        try:
            wazuh_event = from_json(event)
            if ocsf_class == 'DETECTION_FINDING':
                ocsf_event = to_detection_finding(wazuh_event).model_dump()
            else:
                ocsf_event = to_security_finding(wazuh_event).model_dump()
            ocsf_events.append(ocsf_event)
        except Exception as e:
            logging.error(f"Error transforming line to OCSF: {e}")
    return ocsf_events
