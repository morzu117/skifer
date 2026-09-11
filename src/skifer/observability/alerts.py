"""
alerts.py — AlertDispatcher

Sends notifications when a MonitorReport contains failures.
Supported channels: webhook (generic HTTP POST), Slack (webhook), MS Teams,
Google Chat, email (SMTP).

Only sends on failures; INFO-only reports are silently ignored.
Severity filtering: by default, only 'warning' and 'critical' failures trigger alerts.
"""
from __future__ import annotations

import json
import smtplib
from email.mime.text import MIMEText
from typing import Any
from urllib import request as urllib_request
from urllib.error import URLError
import warnings

from skifer.observability.monitor import MonitorReport


class AlertDispatcher:
    """
    Dispatches alerts for MonitorReport failures to one or more channels.

    Config keys (all optional):
        webhook_url   (str): Generic HTTP POST endpoint — receives JSON payload.
        slack_webhook (str): Slack Incoming Webhook URL.
        msteams_webhook (str): Microsoft Teams Incoming Webhook URL.
        google_chat_webhook (str): Google Chat Incoming Webhook URL.
        email         (dict): SMTP config with keys:
                              host, port, user, password, to (list | str), subject_prefix.
        min_severity  (str): Minimum severity to trigger an alert ("info" | "warning" | "critical").
                             Defaults to "warning".

    Example::

        dispatcher = AlertDispatcher()
        dispatcher.dispatch(report, {
            "webhook_url": "https://myserver.com/hooks/skifer",
            "slack_webhook": "https://hooks.slack.com/services/...",
            "min_severity": "critical",
        })
    """

    _SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}

    def dispatch(self, report: MonitorReport, config: dict) -> list[str]:
        """
        Dispatch alerts based on report failures and config.

        Args:
            report: MonitorReport to inspect.
            config: Alert configuration dict (see class docstring).

        Returns:
            List of channel names that were notified (for testing/logging).
        """
        min_sev = config.get("min_severity", "warning")
        min_rank = self._SEVERITY_RANK.get(min_sev, 1)

        failures = [
            r for r in report.failures()
            if self._SEVERITY_RANK.get(r.severity, 1) >= min_rank
        ]

        if not failures:
            return []

        notified: list[str] = []
        payload = self._build_payload(report, failures)

        if config.get("webhook_url"):
            if self._notify_channel(
                "webhook",
                lambda: self._send_webhook(config["webhook_url"], payload),
            ):
                notified.append("webhook")

        if config.get("slack_webhook"):
            if self._notify_channel(
                "slack",
                lambda: self._send_slack(config["slack_webhook"], report, failures),
            ):
                notified.append("slack")

        if config.get("msteams_webhook"):
            if self._notify_channel(
                "msteams",
                lambda: self._send_msteams(config["msteams_webhook"], report, failures),
            ):
                notified.append("msteams")

        if config.get("google_chat_webhook"):
            if self._notify_channel(
                "google_chat",
                lambda: self._send_google_chat(
                    config["google_chat_webhook"], report, failures
                ),
            ):
                notified.append("google_chat")

        if config.get("email"):
            if self._notify_channel(
                "email",
                lambda: self._send_email(config["email"], report, failures),
            ):
                notified.append("email")

        return notified

    def dispatch_incident(
        self,
        *,
        target_fqn: str,
        incidents,
        recipients,
        config: dict,
    ) -> list[str]:
        """Dispatch redacted incident alerts without using the monitor payload builder."""
        incident_list = list(incidents)
        recipient_list = list(recipients)
        if not incident_list:
            return []

        payload = _redacted_incident_payload(target_fqn, incident_list, recipient_list)
        lines = _redacted_incident_lines(target_fqn, incident_list, recipient_list)
        notified: list[str] = []

        if config.get("webhook_url"):
            if self._notify_channel(
                "webhook",
                lambda: self._send_webhook(config["webhook_url"], payload),
            ):
                notified.append("webhook")

        if config.get("slack_webhook"):
            if self._notify_channel(
                "slack",
                lambda: self._send_webhook(
                    config["slack_webhook"], {"text": "\n".join(lines)}
                ),
            ):
                notified.append("slack")

        if config.get("msteams_webhook"):
            if self._notify_channel(
                "msteams",
                lambda: self._send_webhook(
                    config["msteams_webhook"],
                    _redacted_msteams_card(target_fqn, lines),
                ),
            ):
                notified.append("msteams")

        if config.get("google_chat_webhook"):
            if self._notify_channel(
                "google_chat",
                lambda: self._send_webhook(
                    config["google_chat_webhook"], {"text": "\n".join(lines)}
                ),
            ):
                notified.append("google_chat")

        if config.get("email"):
            contacts = [r.contact for r in recipient_list if getattr(r, "contact", None)]
            if contacts or config["email"].get("to"):
                if self._notify_channel(
                    "email",
                    lambda: self._send_redacted_email(
                        config["email"],
                        f"Incident alert: {target_fqn}",
                        "\n".join(lines),
                        recipients=contacts or None,
                    ),
                ):
                    notified.append("email")

        return notified

    # ------------------------------------------------------------------
    # Payload builder
    # ------------------------------------------------------------------

    def _build_payload(self, report: MonitorReport, failures: list) -> dict[str, Any]:
        summary = report.summary()
        return {
            "source": "skifer",
            "table": report.table,
            "timestamp": report.timestamp.isoformat(),
            "status": summary.get("status", "FAIL"),
            "total_checks": summary.get("total_checks", 0),
            "passed": summary.get("passed", 0),
            "failures": [
                {
                    "check": type(r.contract).__name__,
                    "severity": r.severity,
                    "message": r.message,
                    "actual": str(r.actual_value) if r.actual_value is not None else None,
                    "expected": str(r.expected_value) if r.expected_value is not None else None,
                }
                for r in failures
            ],
        }

    # ------------------------------------------------------------------
    # Generic HTTP webhook
    # ------------------------------------------------------------------

    def _send_webhook(self, url: str, payload: dict) -> None:
        """POST the JSON payload to the webhook URL."""
        body = json.dumps(payload).encode("utf-8")
        req = urllib_request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=10):
                pass
        except (URLError, Exception) as exc:
            # Best-effort — log but do not raise
            warnings.warn(f"[AlertDispatcher] webhook POST failed: {exc}", RuntimeWarning)

    # ------------------------------------------------------------------
    # Slack
    # ------------------------------------------------------------------

    def _send_slack(self, webhook_url: str, report: MonitorReport, failures: list) -> None:
        """Send a Slack message via an Incoming Webhook."""
        lines = [f"*Monitor Report — {report.table}*", f"Status: `{report.summary()['status']}`"]
        for r in failures:
            icon = "🚨" if r.severity == "critical" else "⚠️"
            lines.append(f"{icon} {r.message}")

        payload = {"text": "\n".join(lines)}
        self._send_webhook(webhook_url, payload)

    def _send_msteams(self, webhook_url: str, report: MonitorReport, failures: list) -> None:
        """Send a redacted Microsoft Teams MessageCard."""
        self._send_webhook(
            webhook_url,
            _redacted_msteams_card(report.table, _redacted_lines(report, failures)),
        )

    def _send_google_chat(
        self, webhook_url: str, report: MonitorReport, failures: list
    ) -> None:
        """Send a redacted Google Chat message."""
        self._send_webhook(
            webhook_url,
            {"text": "\n".join(_redacted_lines(report, failures))},
        )

    # ------------------------------------------------------------------
    # Email (SMTP)
    # ------------------------------------------------------------------

    def _send_email(self, cfg: dict, report: MonitorReport, failures: list) -> None:
        """Send an email via SMTP."""
        subject_prefix = cfg.get("subject_prefix", "[Skifer]")
        status = report.summary()["status"]
        subject = f"{subject_prefix} {status} — Monitor Report: {report.table}"

        body_lines = [
            f"Monitor Report for table: {report.table}",
            f"Status: {status}",
            f"Timestamp: {report.timestamp.isoformat()}",
            "",
            "Failures:",
        ]
        for r in failures:
            body_lines.append(f"  [{r.severity.upper()}] {r.message}")

        body = "\n".join(body_lines)

        recipients = cfg.get("to", [])
        if isinstance(recipients, str):
            recipients = [recipients]

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = cfg.get("user", "skifer@noreply.local")
        msg["To"] = ", ".join(recipients)

        try:
            with smtplib.SMTP(cfg.get("host", "localhost"), cfg.get("port", 587)) as server:
                if cfg.get("user") and cfg.get("password"):
                    server.starttls()
                    server.login(cfg["user"], cfg["password"])
                server.sendmail(msg["From"], recipients, msg.as_string())
        except Exception as exc:
            warnings.warn(f"[AlertDispatcher] email send failed: {exc}", RuntimeWarning)

    def _send_redacted_email(
        self,
        cfg: dict,
        subject: str,
        body: str,
        *,
        recipients: list[str] | None = None,
    ) -> None:
        targets = recipients if recipients is not None else cfg.get("to", [])
        if isinstance(targets, str):
            targets = [targets]

        msg = MIMEText(body)
        msg["Subject"] = f"{cfg.get('subject_prefix', '[Skifer]')} {subject}"
        msg["From"] = cfg.get("user", "skifer@noreply.local")
        msg["To"] = ", ".join(targets)

        try:
            with smtplib.SMTP(cfg.get("host", "localhost"), cfg.get("port", 587)) as server:
                if cfg.get("user") and cfg.get("password"):
                    server.starttls()
                    server.login(cfg["user"], cfg["password"])
                server.sendmail(msg["From"], targets, msg.as_string())
        except Exception as exc:
            warnings.warn(f"[AlertDispatcher] email send failed: {exc}", RuntimeWarning)

    def _notify_channel(self, channel: str, send) -> bool:
        try:
            send()
        except Exception as exc:  # noqa: BLE001 - alerting is best-effort.
            warnings.warn(
                f"[AlertDispatcher] {channel} send failed: {type(exc).__name__}",
                RuntimeWarning,
            )
            return False
        return True


def _redacted_lines(report, failures) -> list[str]:
    return [f"Table: {report.table}"] + [
        f"[{r.severity.upper()}] {type(r.contract).__name__}" for r in failures
    ]


def _redacted_msteams_card(title_target: str, lines: list[str]) -> dict[str, Any]:
    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"Skifer - {title_target}",
        "themeColor": "D93F0B",
        "title": f"Skifer alert - {title_target}",
        "sections": [{"text": line} for line in lines],
    }


def _redacted_incident_payload(
    target_fqn: str,
    incidents: list,
    recipients: list,
) -> dict[str, Any]:
    return {
        "source": "skifer",
        "event": "incident",
        "target_fqn": target_fqn,
        "incidents": [_redacted_incident_entry(incident) for incident in incidents],
        "recipients": [_redacted_recipient_entry(recipient) for recipient in recipients],
    }


def _redacted_incident_entry(incident) -> dict[str, Any]:
    entry = {
        "id": getattr(incident, "id", None),
        "check_name": getattr(incident, "check_name", None),
        "severity": getattr(incident, "severity", None),
    }
    for key in ("from_version", "to_version"):
        value = getattr(incident, key, None)
        if value is not None:
            entry[key] = value
    return entry


def _redacted_recipient_entry(recipient) -> dict[str, Any]:
    return {
        "contact": getattr(recipient, "contact", None),
        "team": getattr(recipient, "team", None),
        "via": getattr(recipient, "via", None),
        "source_fqn": getattr(recipient, "source_fqn", None),
    }


def _redacted_incident_lines(
    target_fqn: str,
    incidents: list,
    recipients: list,
) -> list[str]:
    lines = [f"Table: {target_fqn}"]
    for incident in incidents:
        line = (
            f"[{str(getattr(incident, 'severity', 'critical')).upper()}] "
            f"{getattr(incident, 'check_name', 'Incident')} "
            f"({getattr(incident, 'id', 'unknown')})"
        )
        from_version = getattr(incident, "from_version", None)
        to_version = getattr(incident, "to_version", None)
        if from_version is not None or to_version is not None:
            line = f"{line} version {from_version or 'unknown'} -> {to_version or 'unknown'}"
        lines.append(line)
    for recipient in recipients:
        contact = getattr(recipient, "contact", None)
        if not contact:
            continue
        team = getattr(recipient, "team", None)
        source_fqn = getattr(recipient, "source_fqn", None)
        owner = f"{team} <{contact}>" if team else contact
        lines.append(f"Owner: {owner}" + (f" ({source_fqn})" if source_fqn else ""))
    return lines
