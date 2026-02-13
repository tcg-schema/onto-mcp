from __future__ import annotations
from typing import Dict, Any, List, Tuple
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, OWL

XSD_NS = "http://www.w3.org/2001/XMLSchema#"

def schema_card_from_ontology_ttl(
    ttl_path: str,
    namespace: str,
) -> Dict[str, Any]:
    g = Graph()
    g.parse(ttl_path, format="turtle")

    # Classes
    classes: List[Dict[str, Any]] = []
    for c in set(g.subjects(RDF.type, OWL.Class)) | set(g.subjects(RDF.type, RDFS.Class)):
        if not isinstance(c, (str,)) and str(c).startswith(namespace):
            label = next(g.objects(c, RDFS.label), None)
            classes.append({
                "name": str(c).replace(namespace, ""),
                "iri": str(c),
                "label": str(label) if label else "",
                "description": ""
            })

    # Properties: separate datatype vs object by range heuristic
    datatype_props: List[Dict[str, Any]] = []
    object_props: List[Dict[str, Any]] = []

    for p in set(g.subjects(RDF.type, OWL.DatatypeProperty)) | set(g.subjects(RDF.type, OWL.ObjectProperty)) | set(g.subjects(RDF.type, RDF.Property)):
        piri = str(p)
        if not piri.startswith(namespace):
            continue

        dom = next(g.objects(p, RDFS.domain), None)
        rng = next(g.objects(p, RDFS.range), None)
        label = next(g.objects(p, RDFS.label), None)

        # classify
        is_datatype = (rng is not None and str(rng).startswith(XSD_NS)) or (p, RDF.type, OWL.DatatypeProperty) in g
        is_object = (p, RDF.type, OWL.ObjectProperty) in g

        item = {
            "name": piri.replace(namespace, ""),
            "iri": piri,
            "domain": str(dom).replace(namespace, "") if dom and str(dom).startswith(namespace) else (str(dom) if dom else ""),
            "range": str(rng).replace(namespace, "") if rng and str(rng).startswith(namespace) else (str(rng) if rng else ""),
            "label": str(label) if label else "",
            "description": ""
        }

        if is_object and not is_datatype:
            object_props.append(item)
        elif is_datatype and not is_object:
            datatype_props.append(item)
        else:
            # unknown/ambiguous: decide by range
            if rng is not None and str(rng).startswith(XSD_NS):
                datatype_props.append(item)
            else:
                object_props.append(item)

    return {
        "namespace": namespace,
        "classes": sorted(classes, key=lambda x: x["name"].lower()),
        "datatype_properties": sorted(datatype_props, key=lambda x: x["name"].lower()),
        "object_properties": sorted(object_props, key=lambda x: x["name"].lower()),
        "aliases": [],
        "warnings": [],
        "source": {"baseline_ontology_ttl": ttl_path},
    }
