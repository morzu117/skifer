"""
reporter.py — MonitorReporter

Generates human-readable and machine-readable reports from a MonitorReport.
Supported formats: JSON, plain text, HTML.
"""
from __future__ import annotations

import json
from pathlib import Path

from skifer.observability.monitor import MonitorReport
from skifer.observability.history import _report_to_dict
from skifer.observability.checks import CheckStatus


class MonitorReporter:
    """
    Converts a MonitorReport to various output formats.

    Usage::

        reporter = MonitorReporter()
        print(reporter.to_text(report))
        data = json.loads(reporter.to_json(report))
        reporter.to_html(report, "report.html")
    """

    def to_json(self, report: MonitorReport) -> str:
        """
        Serialise the MonitorReport (including summary) to a JSON string.

        Returns:
            Pretty-printed JSON string.
        """
        d = _report_to_dict(report)
        d["summary"] = report.summary()
        return json.dumps(d, indent=2, ensure_ascii=False)

    def to_text(self, report: MonitorReport) -> str:
        """
        Generate a human-readable plain-text report.

        Returns:
            Multi-line text string suitable for printing to a terminal or log.
        """
        summary = report.summary()
        status = summary["status"]
        lines = [
            "=" * 60,
            f"  Monitor Report — {report.table}",
            f"  Timestamp : {report.timestamp.isoformat()}",
            f"  Status    : {status}",
            f"  Checks    : {summary['passed']}/{summary['total_checks']} passed",
            "=" * 60,
        ]

        if not report.results:
            lines.append("  (no checks configured)")
        else:
            for r in report.results:
                icon = {
                    CheckStatus.PASS: "✅ PASS",
                    CheckStatus.ERROR: "💥 ERROR",
                    CheckStatus.SKIPPED: "⏭️  SKIPPED",
                }.get(r.status, "🚨 FAIL" if r.severity == "critical" else "⚠️  FAIL")
                check_type = type(r.contract).__name__
                lines.append(f"  {icon}  [{check_type}]  {r.message}")

        lines.append("=" * 60)
        return "\n".join(lines)

    def to_html(self, report: MonitorReport, output_path: str) -> None:
        """
        Write an HTML report to a file.

        Args:
            report:      The MonitorReport to render.
            output_path: Destination file path (will be overwritten if it exists).
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        summary = report.summary()
        status = summary["status"]
        status_color = {"PASS": "#2ecc71", "WARN": "#f39c12", "CRITICAL": "#e74c3c"}.get(
            status, "#95a5a6"
        )

        rows_html = ""
        for r in report.results:
            row_color = "#2ecc71" if r.status is CheckStatus.PASS else (
                "#e74c3c" if r.severity == "critical" else "#f39c12"
            )
            rows_html += (
                f"<tr>"
                f"<td style='color:{row_color};font-weight:bold'>{r.status.value}</td>"
                f"<td>{type(r.contract).__name__}</td>"
                f"<td>{r.severity}</td>"
                f"<td>{r.message}</td>"
                f"<td>{r.actual_value}</td>"
                f"<td>{r.expected_value}</td>"
                f"</tr>\n"
            )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Monitor Report — {report.table}</title>
  <style>
    body {{ font-family: sans-serif; margin: 2em; }}
    h1 {{ color: #2c3e50; }}
    .status {{ color: {status_color}; font-size: 1.4em; font-weight: bold; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th {{ background: #2c3e50; color: white; padding: 8px; text-align: left; }}
    td {{ border: 1px solid #ddd; padding: 8px; }}
    tr:nth-child(even) {{ background: #f8f9fa; }}
  </style>
</head>
<body>
  <h1>Monitor Report</h1>
  <p><strong>Table :</strong> {report.table}</p>
  <p><strong>Timestamp :</strong> {report.timestamp.isoformat()}</p>
  <p class="status">{status} — {summary['passed']}/{summary['total_checks']} checks passed</p>
  <table>
    <thead>
      <tr>
        <th>Status</th><th>Check</th><th>Severity</th><th>Message</th>
        <th>Actual</th><th>Expected</th>
      </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
</body>
</html>"""

        Path(output_path).write_text(html, encoding="utf-8")
