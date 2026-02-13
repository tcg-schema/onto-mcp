# ontorag/schema_extractor_pipeline.py
from __future__ import annotations
from typing import List, Dict, Any

from ontorag.ontology_extractor_openrouter import extract_ontology_chunk_proposals
from ontorag.proposal_aggregator import aggregate_chunk_proposals


def extract_schema_proposals(
    chunks: List[Dict[str, Any]],
    schema_card_for_prompt: Dict[str, Any],
) -> Dict[str, Any]:
    """
    High-level pipeline:

    chunks -> chunk ontology proposals -> aggregated proposal JSON

    Returns a proposal artifact ready to be written to disk and later
    merged into a schema card.
    """

    # 1. Extract chunk-level ontology proposals via OpenRouter LLM
    chunk_proposals = extract_ontology_chunk_proposals(
        chunks,
        schema_card_for_prompt,
    )

    # 2. Aggregate chunk proposals into a single proposal artifact
    aggregated = aggregate_chunk_proposals(chunk_proposals)

    # 3. Wrap into proposal artifact format
    proposal = {
        "namespace": schema_card_for_prompt.get("namespace"),
        "proposed_additions": {
            "classes": aggregated.get("classes", []),
            "datatype_properties": aggregated.get("datatype_properties", []),
            "object_properties": aggregated.get("object_properties", []),
            "events": aggregated.get("events", []),
        },
        "alias_or_merge_suggestions": aggregated.get("merge_suggestions", []),
        "warnings": aggregated.get("warnings", []),
        "meta": {
            "source": "ontorag.extract-schema",
            "num_chunks": len(chunks),
        }
    }

    return proposal
