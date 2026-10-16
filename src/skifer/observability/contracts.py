"""
contracts.py — ContractExtractor

Derives a list of DataContract objects from a normalized schema dict.
The YAML is the contract — no additional configuration required.

Mapping rules (Phase 1):
  quality_checks.drop_nulls_in      → NullCheck (severity: critical)
  quality_checks.drop_duplicates_on → UniqueCheck (severity: critical)
  filter[].operator                 → FilterInvariantCheck (severity: warning),
                                      only for a column the output publishes
  select_final[].ops[cast:type]     → TypeCheck (severity: warning)

Mapping rules (Phase 2 — observability: section):
  observability.freshness           → FreshnessCheck
  observability.volume              → VolumeCheck + VolumeVariationCheck
  observability.schema_drift        → SchemaDriftCheck
  observability.custom_checks       → CustomSqlCheck
"""
from __future__ import annotations

from skifer.observability.checks import (
    DataContract,
    NullCheck,
    UniqueCheck,
    TypeCheck,
    FilterInvariantCheck,
    FreshnessCheck,
    LoadFreshnessCheck,
    VolumeCheck,
    VolumeVariationCheck,
    SchemaDriftCheck,
    CustomSqlCheck,
)


class ContractExtractor:
    """Derives DataContract objects from a normalized schema dict."""

    def extract(self, schema_dict: dict) -> list[DataContract]:
        """
        Parse a normalized schema dict and return all derived contracts.

        Args:
            schema_dict: Output of parse_schema() / load_schema().

        Returns:
            List of DataContract instances (NullCheck, UniqueCheck,
            FilterInvariantCheck, TypeCheck).
        """
        contracts: list[DataContract] = []
        outputs = _output_columns(schema_dict)

        # --- Derive contracts from each table entry -----------------------
        for table in schema_dict.get("tables", []):
            fqn = table.get("name", "unknown")
            qc = table.get("quality_checks", {})

            # drop_nulls_in → NullCheck (critical)
            for col in qc.get("drop_nulls_in", []):
                contracts.append(NullCheck(table=fqn, column=col, severity="critical"))

            # drop_duplicates_on → UniqueCheck (critical)
            dup_cols = qc.get("drop_duplicates_on", [])
            if dup_cols:
                contracts.append(UniqueCheck(table=fqn, columns=dup_cols, severity="critical"))

            # filter → FilterInvariantCheck (warning)
            for f in table.get("filter", []):
                # A filter on a column the output drops cannot be checked on it:
                # the query errors on every run. Only checkable invariants are kept.
                if isinstance(f, dict) and (outputs is None or f.get("column") in outputs):
                    contracts.append(
                        FilterInvariantCheck(
                            table=fqn,
                            column=f.get("column", ""),
                            operator=f.get("operator", ""),
                            value=f.get("value"),
                            severity="warning",
                        )
                    )

        # --- Derive TypeChecks from select_final --------------------------
        # Use the first table as the reference FQN (most common case)
        first_table_fqn = (schema_dict.get("tables") or [{}])[0].get("name", "unknown")

        for col_def in schema_dict.get("select_final", []):
            target_col = None
            ops = []
            if isinstance(col_def, list) and len(col_def) >= 3:
                target_col = col_def[1]
                ops = col_def[2] if isinstance(col_def[2], list) else [col_def[2]]
            elif isinstance(col_def, dict):
                target_col = col_def.get("target")
                ops = col_def.get("ops", [])
            if not target_col:
                continue
            for op in ops:
                if isinstance(op, str) and op.startswith("cast:"):
                    expected_type = op[len("cast:"):]
                    contracts.append(
                        TypeCheck(
                            table=first_table_fqn,
                            column=target_col,
                            expected_type=expected_type,
                            severity="warning",
                        )
                    )

        # --- Derive contracts from observability: section -----------------
        observability_cfg = schema_dict.get("observability", {})
        if observability_cfg:
            obs_contracts = self._extract_observability(first_table_fqn, observability_cfg, schema_dict)
            contracts.extend(obs_contracts)

        sla = (schema_dict.get("contract") or {}).get("sla") or {}
        max_latency = sla.get("max_latency")
        if max_latency:
            try:
                FreshnessCheck._parse_delay(str(max_latency))
            except ValueError:
                pass
            else:
                contracts.append(
                    LoadFreshnessCheck(
                        table=first_table_fqn,
                        max_delay=str(max_latency),
                        severity="critical",
                    )
                )

        return contracts

    def _extract_observability(
        self, fqn: str, cfg: dict, schema_dict: dict
    ) -> list[DataContract]:
        """Parse the optional 'observability:' section and return additional contracts."""
        contracts: list[DataContract] = []

        # freshness
        freshness = cfg.get("freshness")
        if freshness:
            contracts.append(
                FreshnessCheck(
                    table=fqn,
                    timestamp_column=freshness.get("timestamp_column", "updated_at"),
                    max_delay=str(freshness.get("max_delay", "24h")),
                    severity=freshness.get("severity", "warning"),
                )
            )

        # volume
        volume = cfg.get("volume")
        if volume:
            contracts.append(
                VolumeCheck(
                    table=fqn,
                    min_rows=volume.get("min_rows"),
                    max_rows=volume.get("max_rows"),
                    severity=volume.get("severity", "warning"),
                )
            )
            if volume.get("variation_threshold") is not None:
                contracts.append(
                    VolumeVariationCheck(
                        table=fqn,
                        variation_threshold=float(volume["variation_threshold"]),
                        severity=volume.get("severity", "warning"),
                    )
                )

        # schema_drift
        schema_drift = cfg.get("schema_drift")
        if schema_drift and schema_drift.get("enabled", False):
            # Derive expected columns from select_final
            expected_cols = []
            for col_def in schema_dict.get("select_final", []):
                if isinstance(col_def, list) and len(col_def) >= 2:
                    expected_cols.append(col_def[1])
                elif isinstance(col_def, dict):
                    expected_cols.append(col_def.get("target", ""))
            contracts.append(
                SchemaDriftCheck(
                    table=fqn,
                    expected_columns=expected_cols,
                    severity=schema_drift.get("severity", "warning"),
                )
            )

        # custom_checks
        for custom in cfg.get("custom_checks", []):
            contracts.append(
                CustomSqlCheck(
                    table=fqn,
                    sql=custom.get("sql", ""),
                    expect=custom.get("expect", 0),
                    severity=custom.get("severity", "warning"),
                )
            )

        return contracts


def _output_columns(schema_dict: dict) -> set[str] | None:
    """Names the published table carries, or ``None`` when the schema does not say."""
    aggregate = schema_dict.get("aggregate")
    if isinstance(aggregate, dict):
        columns = set(aggregate.get("group_by") or [])
        for measure in aggregate.get("measures") or []:
            if isinstance(measure, dict) and measure.get("target"):
                columns.add(measure["target"])
        return columns
    select_final = schema_dict.get("select_final")
    if not select_final or schema_dict.get("keep_all_columns"):
        return None
    columns = set()
    for entry in select_final:
        if isinstance(entry, str):
            columns.add(entry)
        elif isinstance(entry, list) and entry:
            columns.add(entry[1] if len(entry) >= 2 else entry[0])
        elif isinstance(entry, dict):
            columns.add(entry.get("target") or entry.get("source"))
    return columns
