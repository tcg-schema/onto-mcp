from __future__ import annotations
from typing import Dict, Any, Tuple
from rdflib import Graph
from rdflib.namespace import RDF, RDFS

def build_instances_index(instances_ttl: str, namespace: str) -> Dict[str, Any]:
    g = Graph()
    g.parse(instances_ttl, format="turtle")

    by_label: Dict[Tuple[str, str], str] = {}  # (classNameLower, labelLower) -> iri
    by_any_label: Dict[str, str] = {}          # labelLower -> iri (fallback)
    class_of: Dict[str, str] = {}              # iri -> classIri
    label_of: Dict[str, str] = {}              # iri -> label

    for s, _, cls in g.triples((None, RDF.type, None)):
        s_iri = str(s)
        cls_iri = str(cls)
        class_of[s_iri] = cls_iri

        lbl = next(g.objects(s, RDFS.label), None)
        if lbl:
            lab = str(lbl).strip()
            label_of[s_iri] = lab
            lkey = lab.lower()
            by_any_label.setdefault(lkey, s_iri)
            if cls_iri.startswith(namespace):
                cname = cls_iri.replace(namespace, "").lower()
                by_label[(cname, lkey)] = s_iri

    return {
        "by_label": by_label,
        "by_any_label": by_any_label,
        "class_of": class_of,
        "label_of": label_of,
    }
