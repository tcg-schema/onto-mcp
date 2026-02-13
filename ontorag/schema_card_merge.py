from __future__ import annotations
from typing import Dict, Any, List, Tuple

def _key(x: str) -> str:
    return (x or "").strip().lower()

def merge_schema_cards(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    # overlay wins (current schema card > baseline TTL)
    out = dict(base)
    out["namespace"] = overlay.get("namespace") or base.get("namespace")

    def merge_list_by_name(base_list, over_list):
        idx = {_key(i.get("name")): i for i in base_list or [] if isinstance(i, dict)}
        for it in over_list or []:
            if not isinstance(it, dict):
                continue
            idx[_key(it.get("name"))] = it
        return list(idx.values())

    out["classes"] = merge_list_by_name(base.get("classes", []), overlay.get("classes", []))
    out["datatype_properties"] = merge_list_by_name(base.get("datatype_properties", []), overlay.get("datatype_properties", []))
    out["object_properties"] = merge_list_by_name(base.get("object_properties", []), overlay.get("object_properties", []))

    # these are append-ish
    out["aliases"] = (base.get("aliases", []) or []) + (overlay.get("aliases", []) or [])
    out["warnings"] = (base.get("warnings", []) or []) + (overlay.get("warnings", []) or [])
    return out
