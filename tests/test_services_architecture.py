"""Architecture and compatibility tests for the services extraction."""

from pathlib import Path

import skifer.agentic.data_service as data_service
import skifer.services as services


def test_mcp_does_not_import_agentic_data_service():
    mcp_dir = Path(__file__).parents[1] / "src" / "skifer" / "mcp"
    for module_path in mcp_dir.glob("*.py"):
        assert "agentic.data_service" not in module_path.read_text(encoding="utf-8")


def test_services_reexport_matches_data_service_identity():
    assert services.RequestContext is data_service.RequestContext
    assert services.ServiceLimits is data_service.ServiceLimits
    assert services.ScopeDenied is data_service.ScopeDenied
