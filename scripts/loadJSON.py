import json




def transform_domain(data):
    """Transform the loaded JSON data into the desired Python structure."""
    domain = {"entities": {}, "relationships": {}}

    # Transform entities
    for key, entity in data.get("entities", {}).items():
        domain["entities"][key] = {
            "name": entity["name"],
            "card": entity["card"],
            "attr": {
                attr_key: {
                    "card": attr["card"],
                    "type": attr["type"],
                    "len": attr["len"],
                    "pk": attr["pk"],
                    "fk": attr["fk"],
                }
                for attr_key, attr in entity.get("attr", {}).items()
            },
        }

    # Transform relationships
    for key, relationship in data.get("relationships", {}).items():
        domain["relationships"][key] = {
            "from": relationship["from"],
            "to": relationship["to"],
            "avgCard": relationship["avgCard"],
            "maxCard": relationship["maxCard"],
            "fromAtt": relationship["fromAtt"],
            "toAtt": relationship["toAtt"],
        }

    return domain

def load_domain(filename):
    """Load a JSON file and transform its content into the desired structure."""
    with open(filename, 'r') as file:
        data = json.load(file)
    return transform_domain(data)


def _convert_collection_tree(tree):
    """
    Recursively convert a collection definition from JSON into the internal tree
    format expected by the planning utilities. Treat empty dicts as leaves and
    convert them to None so that in JSON saranno null.
    """
    if isinstance(tree, dict):
        if not tree:
            return None
        return {k: _convert_collection_tree(v) for k, v in tree.items()}
    if tree is None:
        return None
    if isinstance(tree, (list, tuple, set)):
        # mantieni la struttura come insieme di nodi convertiti
        return {_convert_collection_tree(v) for v in tree}
    return tree


def _serialize_collection_tree(tree):
    if isinstance(tree, dict):
        return {k: _serialize_collection_tree(v) for k, v in tree.items()}
    if tree is None:
        return None
    if isinstance(tree, (set, list, tuple)):
        return [_serialize_collection_tree(v) for v in tree]
    return tree

def _find_collection_key(collections, c_value):
    # If already a key name, return it
    if isinstance(c_value, str):
        return c_value
    # Try direct equality with collection values
    for k, v in collections.items():
        if v == c_value:
            return k
    # Try matching serialized forms (helps if types differ e.g. list vs set)
    serialized_target = _serialize_collection_tree(c_value)
    for k, v in collections.items():
        if _serialize_collection_tree(v) == serialized_target:
            return k
    raise ValueError("Collection value not found among provided collections keys")

def _denormalize_normalized_indexes(normalized):
    """
    Inverte la conversione fatta da normalize_existing_indexes:
    input: normalized è un dict { coll: set( tuple((ent,attr), ...) ), ... }
    output: dict nello stesso formato JSON originale: { coll: { "IX": [ [ [ent,attr], ...], ... ] }, ... }
    """
    out = {}
    for coll, idx_set in normalized.items():
        ix_list = []
        for idx in idx_set:
            # ogni idx è una tupla di coppie (ent,attr)
            ix_list.append([ list(item) for item in idx ])
        out[coll] = {"IX": ix_list}
    return out

def save_solution(filename, collections, idxs, qps):
    """
    Serializza collections, indexes e qps nel formato JSON che load_solution
    si aspetta come input e salva su file.
    """
    data = {}

    # Collections: convert sets/tuples back to lists, mantenendo la struttura annidata
    collections=_convert_collection_tree(collections)
    data["collections"] = {k: _serialize_collection_tree(v) for k, v in collections.items()}

    # Indexes: convert tuple attributes to liste
    indexes=_denormalize_normalized_indexes(idxs)
    data["indexes"] = {}
    for key, value in indexes.items():
        ix_list = []
        for ix in value.get("IX", []):
            ix_list.append([list(attr) for attr in ix])
        data["indexes"][key] = {"IX": ix_list}

    # Query plans: riportare "c" al nome della collection, convertire tuple->list per sp/jp
    data["query_plans"] = {}
    for key, value in qps.items():
        aps_out = []
        for ap in value.get("aps", []):
            
            try:
                c_key = _find_collection_key(collections, ap.get("c"))
            except ValueError:
                # se non si trova, prova a usare il valore originale (se è stringa) o None
                c_key = ap.get("c") if isinstance(ap.get("c"), str) else None

            ap_out = {"c": c_key, "r": ap.get("r")}
            # sp: sempre lista (può essere vuota)
            ap_out["sp"] = [list(sp) for sp in ap.get("sp", [])]
            # jp: includi solo se presente e non None
            if ap.get("jp") is not None:
                ap_out["jp"] = [list(jp) for jp in ap.get("jp")]
            aps_out.append(ap_out)

        data["query_plans"][key] = {"freq": value.get("freq"), "type": value.get("type"), "aps": aps_out}

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)



def load_solution(filename):
    """Load a JSON file and transform its content into the desired Python structure."""
    with open(filename, 'r') as file:
        data = json.load(file)

    # Transform collections, keeping nesting structure
    collections = {
        key: _convert_collection_tree(value)
        for key, value in data.get("collections", {}).items()
    }

    # Transform indexes
    indexes = {
        key: {
            "IX": [[tuple(attr) for attr in ix] for ix in value.get("IX", [])]
        }
        for key, value in data.get("indexes", {}).items()
    }

    # Transform query plans
    qps = {
        key: {
            "freq": value["freq"],
            "type": value["type"],
            "aps": [
                {
                   # "c": collections[ap["c"]],
                    "c": ap["c"],
                    "r": ap["r"],
                    "sp": [tuple(sp) for sp in ap.get("sp", [])],
                    "jp": [tuple(jp) for jp in ap.get("jp", [])] if ap.get("jp") else None,
                }
                for ap in value.get("aps", [])
            ],
        }
        for key, value in data.get("query_plans", {}).items()
    }

    return collections, indexes, qps

def load_workload(filename):
    """Load a JSON file and return its content."""
    with open(filename, 'r') as file:
        data = json.load(file)
    return data


def main():
    domain = load_domain('json/domain.json')
    collections, indexes, qps = load_solution('json/alpha.json')
    worload=load_workload('json/workloadChen.json')

    print("Domain:")
    print(domain)
    print("Workload:")
    print(worload)
    print("\nCollections:")
    print(collections)
    print("\nIndexes:")
    print(indexes)
    print("\nQuery Plans:")
    print(qps)

if __name__ == "__main__":
    main()
