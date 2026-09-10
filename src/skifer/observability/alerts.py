"""
alerts.py — AlertDispatcher

Sends notifications when a MonitorReport contains failures.
Supported channels: webhook (generic HTTP POST), Slack (webhook), email (SMTP).

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

from skifer.observability.monitor import MonitorReport


class AlertDispatcher:
    """
    Dispatches alerts for MonitorReport failures to one or more channels.

    Config keys (all optional):
        webhook_url   (str): Generic HTTP POST endpoint — receives JSON payload.
        slack_webhook (str): Slack Incoming Webhook URL.
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

        # Generic webhook
        if config.get("webhook_url"):
            self._send_webhook(config["webhook_url"], payload)
            notified.append("webhook")

        # Slack
        if config.get("slack_webhook"):
            self._send_slack(config["slack_webhook"], report, failures)
            notified.append("slack")

        # Email
        if config.get("email"):
            self._send_email(config["email"], report, failures)
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
            import warnings
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
            import warnings
            warnings.warn(f"[AlertDispatcher] email send failed: {exc}", RuntimeWarning)

