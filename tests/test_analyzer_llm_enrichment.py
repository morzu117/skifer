"""Tests for the constrained LLM enrichment of the project analyzer (Plan 43
— 43.6). The LLM is always mocked — a ``FakeProvider`` stands in for
``LLMProvider``, never a live call, matching every other LLM-touching test
in this repo. Fully Spark-free.
"""
from __future__ import annotations

import json

from skifer.analyzer.llm_enrichment import EnrichmentResult, enrich
from skifer.analyzer.patterns import PatternReport, RuleOccurrence, SharedRuleCandidate


class FakeProvider:
    """Returns pre-canned JSON strings, one per call, in order."""

    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def complete(self, *, system_prompt, user_message, temperature=0.0, **kwargs):
        self.calls.append({"system_prompt": system_prompt, "user_message": user_message})
        return self._responses.pop(0)


def _occurrence(alias="country", sql_text="UPPER(TRIM(country))") -> RuleOccurrence:
    return RuleOccurrence(
        source_path="a.sql", line=1, output_alias=alias, sql_text=sql_text, literal_values=(),
    )


def _candidate(name="country", sql_text="UPPER(TRIM(country))") -> SharedRuleCandidate:
    return SharedRuleCandidate(
        name=name, kind="projection", fingerprint="sha256:v1:x",
        occurrences=(_occurrence(sql_text=sql_text), _occurrence(sql_text=sql_text)),
    )


class TestNoProviderIsASilentNoOp:
    def test_every_field_is_empty(self):
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        result = enrich(report, {"silver/t.yaml": ["id"]}, provider=None)

        assert result == EnrichmentResult()
        assert result.to_dict() == {"rules": [], "drafts": [], "groupings": []}


class TestRuleEnrichment:
    def test_valid_response_is_accepted(self):
        provider = FakeProvider([json.dumps({"name": "standardize_country", "docstring": "Uppercases and trims country."})])
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        result = enrich(report, {}, provider=provider)

        assert len(result.rules) == 1
        rule = result.rules[0]
        assert rule.candidate_name == "country"
        assert rule.proposed_name == "standardize_country"
        assert rule.docstring == "Uppercases and trims country."
        assert rule.rejected_reason is None

    def test_never_sends_table_names_or_literal_values(self):
        provider = FakeProvider([json.dumps({"name": "x", "docstring": "d"})])
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        enrich(report, {}, provider=provider)

        sent = provider.calls[0]["user_message"] + provider.calls[0]["system_prompt"]
        assert "bronze" not in sent.lower()
        assert "secret_customer_id_42" not in sent

    def test_a_key_outside_the_allowlist_rejects_the_whole_response(self):
        provider = FakeProvider([json.dumps({"name": "x", "docstring": "d", "join_condition": "a.id = b.id"})])
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        result = enrich(report, {}, provider=provider)

        rule = result.rules[0]
        assert rule.proposed_name is None
        assert rule.docstring is None
        assert "join_condition" in rule.rejected_reason

    def test_a_proposed_name_violating_the_identifier_regex_is_rejected(self):
        provider = FakeProvider([json.dumps({"name": "not a valid name!", "docstring": "d"})])
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        result = enrich(report, {}, provider=provider)

        rule = result.rules[0]
        assert rule.proposed_name is None
        assert "does not match" in rule.rejected_reason

    def test_malformed_json_is_rejected_with_a_reason(self):
        provider = FakeProvider(["not json at all"])
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        result = enrich(report, {}, provider=provider)

        assert result.rules[0].rejected_reason is not None

    def test_proposed_name_is_never_used_as_the_candidate_identity(self):
        """The dataclass has no field that could let a caller mistake the
        proposal for the real, wiring-relevant candidate name."""
        provider = FakeProvider([json.dumps({"name": "renamed_thing"})])
        report = PatternReport(shared_rule_candidates=(_candidate(name="country"),))

        result = enrich(report, {}, provider=provider)

        assert result.rules[0].candidate_name == "country"
        assert result.rules[0].proposed_name == "renamed_thing"
        # Advisory only: nothing downstream reads `proposed_name` as identity.


class TestDraftEnrichment:
    def test_valid_description_is_accepted(self):
        provider = FakeProvider([json.dumps({"description": "Customer master records."})])
        report = PatternReport()

        result = enrich(report, {"silver/customers.yaml": ["id", "name", "country"]}, provider=provider)

        assert len(result.drafts) == 1
        assert result.drafts[0].path == "silver/customers.yaml"
        assert result.drafts[0].description == "Customer master records."

    def test_drafts_without_known_columns_still_get_a_call(self):
        provider = FakeProvider([json.dumps({"description": "Unknown shape."})])
        report = PatternReport()

        enrich(report, {"silver/kept_all.yaml": []}, provider=provider)

        assert "(none declared)" in provider.calls[0]["user_message"]

    def test_extra_key_rejects_the_description(self):
        provider = FakeProvider([json.dumps({"description": "x", "table": "silver.customers"})])
        report = PatternReport()

        result = enrich(report, {"silver/customers.yaml": ["id"]}, provider=provider)

        assert result.drafts[0].description is None
        assert "table" in result.drafts[0].rejected_reason


class TestGroupingProposals:
    def test_fewer_than_two_candidates_never_calls_the_provider_for_grouping(self):
        provider = FakeProvider([json.dumps({"name": "x", "docstring": "d"})])
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        result = enrich(report, {}, provider=provider)

        assert result.groupings == ()
        assert len(provider.calls) == 1  # only the one rule-enrichment call, no grouping call

    def test_a_valid_grouping_is_kept(self):
        candidates = (_candidate(name="country_std"), _candidate(name="country_upper"))
        provider = FakeProvider([
            json.dumps({"name": "x", "docstring": "d"}),
            json.dumps({"name": "y", "docstring": "d"}),
            json.dumps({"groups": [{"label": "country normalization", "candidate_names": ["country_std", "country_upper"]}]}),
        ])
        report = PatternReport(shared_rule_candidates=candidates)

        result = enrich(report, {}, provider=provider)

        assert len(result.groupings) == 1
        assert result.groupings[0].label == "country normalization"
        assert set(result.groupings[0].candidate_names) == {"country_std", "country_upper"}

    def test_a_name_not_among_the_real_candidates_is_dropped(self):
        candidates = (_candidate(name="country_std"), _candidate(name="country_upper"))
        provider = FakeProvider([
            json.dumps({"name": "x", "docstring": "d"}),
            json.dumps({"name": "y", "docstring": "d"}),
            json.dumps({"groups": [{"label": "x", "candidate_names": ["country_std", "made_up_name"]}]}),
        ])
        report = PatternReport(shared_rule_candidates=candidates)

        result = enrich(report, {}, provider=provider)

        # Only one real name survives -> below the 2-name minimum -> dropped entirely.
        assert result.groupings == ()

    def test_malformed_groups_response_yields_no_proposals_not_an_exception(self):
        candidates = (_candidate(name="a"), _candidate(name="b"))
        provider = FakeProvider([
            json.dumps({"name": "x", "docstring": "d"}),
            json.dumps({"name": "y", "docstring": "d"}),
            json.dumps({"groups": "not a list"}),
        ])
        report = PatternReport(shared_rule_candidates=candidates)

        result = enrich(report, {}, provider=provider)

        assert result.groupings == ()


class TestEnrichmentResultToDict:
    def test_to_dict_never_contains_a_table_name_or_literal(self):
        provider = FakeProvider([
            json.dumps({"name": "standardize_country", "docstring": "doc"}),
            json.dumps({"description": "desc"}),
        ])
        report = PatternReport(shared_rule_candidates=(_candidate(),))

        result = enrich(report, {"silver/customers.yaml": ["id"]}, provider=provider)
        serialized = json.dumps(result.to_dict())

        assert "bronze" not in serialized
        assert "customers_raw" not in serialized
