#funzioni per generare query plans a partire da dominio, collezioni e query

from loadJSON import load_domain, load_solution,load_workload


def extract_entities_from_collection_tree(tree):
    """
    Ritorna l'insieme delle entità presenti in una definizione di collection,
    gestendo sia dizionari annidati che set/list/tuple.
    Esempi accettati:
      {"Cu": {"Or": {"Py": None}}}
      {"Pr": {"Su", "It"}}
    """
    def visit(node):
        if isinstance(node, dict):
            ents = set()
            for ent, sub in node.items():
                ents.add(ent)
                ents |= visit(sub)
            return ents
        elif isinstance(node, (set, list, tuple)):
            ents = set()
            for el in node:
                if isinstance(el, str):
                    # elementi tipo {"Pr": {"Su","It"}}
                    ents.add(el)
                else:
                    ents |= visit(el)
            return ents
        else:
            # None, stringhe “foglia” già gestite sopra, ecc.
            return set()

    return visit(tree)

def extract_relationships_from_collection_tree(tree, relationships_info):
    """
    Dato l'albero di una collection, es.:
        {"Co": {"C2": {"Us": None}}}
    e domain["relationships"], restituisce
    l'insieme dei nomi di relazioni interne alla collection.
    """
    rels = set()

    def rec(node, parent_entity=None):
        if not isinstance(node, dict):
            return
        for entity, subtree in node.items():
            if parent_entity is not None:
                # arco parent_entity -> entity nell'albero
                candidates = []
                for rname, info in relationships_info.items():
                    e_from = info["from"]
                    e_to = info["to"]
                    if ((e_from == parent_entity and e_to == entity) or
                        (e_from == entity and e_to == parent_entity)):
                        candidates.append(rname)

                if len(candidates) == 1:
                    rels.add(candidates[0])
                elif len(candidates) > 1:
                    raise ValueError(
                        f"Ambiguità: più relazioni tra {parent_entity} e {entity}: {candidates}"
                    )
                # se 0 candidates → nessuna relazione diretta nel dominio

            if isinstance(subtree, dict) and subtree:
                rec(subtree, entity)

    rec(tree, parent_entity=None)
    return rels




def _get_rel_between(relationships_info, parent, child):
    candidates = []
    for rname, info in relationships_info.items():
        if (info["from"] == parent and info["to"] == child) or (info["from"] == child and info["to"] == parent):
            candidates.append(rname)
    return candidates[0] if len(candidates) == 1 else None


def _collect_entity_parent_rels(tree, relationships_info):
    occurrences = {}

    def rec(node, parent_entity=None):
        if isinstance(node, dict):
            for ent, sub in node.items():
                rel = _get_rel_between(relationships_info, parent_entity, ent) if parent_entity else None
                occurrences.setdefault(ent, []).append(rel)
                rec(sub, ent)
        elif isinstance(node, (set, list, tuple)):
            for el in node:
                if isinstance(el, dict):
                    rec(el, parent_entity)
                else:
                    rel = _get_rel_between(relationships_info, parent_entity, el) if parent_entity else None
                    occurrences.setdefault(el, []).append(rel)
        else:
            if parent_entity:
                rel = _get_rel_between(relationships_info, parent_entity, node)
            else:
                rel = None
            occurrences.setdefault(node, []).append(rel)

    rec(tree, parent_entity=None)
    return occurrences


def _annotate_predicates_for_collection(domain, coll_tree, preds, query_rels):
    if not preds:
        return preds
    occurrences = _collect_entity_parent_rels(coll_tree, domain["relationships"])
    if not any(len(v) > 1 for v in occurrences.values()):
        return preds
    qrels = set(query_rels or [])
    if not qrels:
        return preds
    new_preds = []
    for sp in preds:
        if len(sp) >= 4:
            new_preds.append(sp)
            continue
        ent = sp[0]
        if ent in occurrences and len(occurrences[ent]) > 1:
            candidates = {r for r in occurrences[ent] if r in qrels}
            if len(candidates) == 1:
                rel = next(iter(candidates))
                new_preds.append([sp[0], sp[1], sp[2], rel])
                continue
        new_preds.append(sp)
    return new_preds

def generate_write_query_plans_from_query(domain, collections, query_name, query):
    """
    Genera un query plan di tipo 'w' per update usando i pred della query.
    
    - collections: dict delle collections (interno)
    - query_name: nome della query (es. "Q18")
    - query: dict che contiene almeno "f","type"=="w","entities" e "pred"

    Restituisce dict { 'qp_<query_name_lower>': { 'freq':..., 'type':'w', 'aps':[ ... ] } }
    Ogni AP per ogni collection che contiene l'entità target include lo sp preso dalla pred corrispondente.
    """
    # determina entità target (primo elemento di "entities" se presente)
    target =  query.get("entities")[0]

    # trova il pred corrispondente per target (es. ["Or","idO",1])
    sp_entry = query.get("pred", [])[0]
    
    # mappa collection -> set di entità (usa funzione esistente)
    coll_entities_map = {
        name: extract_entities_from_collection_tree(t)
        for name, t in collections.items()
    }

    aps = []
    for c_name, ents in coll_entities_map.items():
        if target in ents:
            occurrences = _collect_entity_parent_rels(collections[c_name], domain["relationships"]).get(target, [])
            if not occurrences:
                occurrences = [None]
            for rel in occurrences:
                # For writes, multiple occurrences are cumulative updates, not alternatives.
                sp = [sp_entry.copy()]
                if rel is not None:
                    sp = [[sp_entry[0], sp_entry[1], sp_entry[2], rel]]
                aps.append({"c": c_name, "r": None, "sp": sp, "jp": None})

    if not aps:
        return {}

    qp_full_name = f"qp_{query_name.lower()}"
    return {
        qp_full_name: {
            "freq": query.get("f", 1),
            "type": "w",
            "aps": aps
        }
    }


def generate_read_query_plans(domain, collections, query_name, query):
    """
    Variante sequenziale: percorre le relazioni nell'ordine della query ma, a ogni
    passo, prende la prima relazione attaccabile (end-point già visitato o interna)
    mantenendo l'ordine relativo. Se la relazione non è interna, crea comunque un
    AP esplicito anche riusando collection.
    """

    relationships_info = domain["relationships"]

    coll_entities_map = {
        name: extract_entities_from_collection_tree(t)
        for name, t in collections.items()
    }
    coll_names_sorted = sorted(coll_entities_map.keys())

    coll_rels_map = {
        name: extract_relationships_from_collection_tree(t, relationships_info)
        for name, t in collections.items()
    }

    pred_list = query.get("pred", [])
    rels_list = query.get("rels", [])

    plans = []

    first_pred_entity = pred_list[0][0] if pred_list else None
    first_rel_endpoints = None
    if rels_list:
        r0 = relationships_info[rels_list[0]]
        first_rel_endpoints = {r0["from"], r0["to"]}

    start_collections = []
    for coll_name, ents in coll_entities_map.items():
        if first_pred_entity is not None and first_pred_entity not in ents:
            continue
        if first_pred_entity is None and first_rel_endpoints is not None:
            if not (first_rel_endpoints & ents):
                continue
        start_collections.append(coll_name)

    # Each start collection creates an independent search over the same rel order.
    for coll_name in sorted(start_collections):
        coll_ents = coll_entities_map[coll_name]

        # Assign predicates to the starting collection and disambiguate if needed.
        start_sp, remaining_pred = [], []
        for p in pred_list:
            (start_sp if p[0] in coll_ents else remaining_pred).append(p)
        start_sp = _annotate_predicates_for_collection(domain, collections[coll_name], start_sp, rels_list)

        visited = set(coll_ents)
        aps = [{
            "c": coll_name,
            "r": None,
            "sp": start_sp,
            "jp": None
        }]

        # BFS-style expansion: each state is a partial plan with pending relations.
        active = [{
            "visited": visited,
            "aps": aps,
            "remaining_pred": remaining_pred,
            "pending_rels": list(rels_list)
        }]
        plans_for_start = []

        while active:
            next_active = []
            for state in active:
                pending = state["pending_rels"]
                if not pending:
                    if not state["remaining_pred"]:
                        plans_for_start.append(state["aps"])
                    continue

                used_colls = {ap["c"] for ap in state["aps"] if ap["c"] is not None}

                # Find the first attachable relation (keeps the original order).
                idx_attach = None
                rname_attach = None
                for idx, rname in enumerate(pending):
                    internal = any(rname in coll_rels_map.get(c, set()) for c in used_colls)
                    info = relationships_info[rname]
                    attachable = info["from"] in state["visited"] or info["to"] in state["visited"]
                    if internal or attachable:
                        idx_attach, rname_attach = idx, rname
                        break

                if rname_attach is None:
                    continue

                info = relationships_info[rname_attach]
                e_from, e_to = info["from"], info["to"]
                from_att, to_att = info["fromAtt"], info["toAtt"]
                from_visited = e_from in state["visited"]
                to_visited = e_to in state["visited"]

                # Internal relation: no AP needed, just consume the relation.
                if any(rname_attach in coll_rels_map.get(c, set()) for c in used_colls):
                    new_pending = pending[:idx_attach] + pending[idx_attach+1:]
                    next_active.append({
                        "visited": state["visited"],
                        "aps": state["aps"],
                        "remaining_pred": state["remaining_pred"],
                        "pending_rels": new_pending
                    })
                    continue

                # Otherwise add an explicit AP; if join is ambiguous, branch per occurrence.
                for coll_candidate in coll_names_sorted:
                    ents = coll_entities_map[coll_candidate]
                    jp = None
                    internal_entity = None
                    internal_attr = None
                    external_entity = None
                    external_attr = None

                    if from_visited and not to_visited and e_to in ents:
                        jp = [[e_from, from_att], [e_to, to_att]]
                        external_entity, external_attr = e_from, from_att
                        internal_entity, internal_attr = e_to, to_att
                    elif to_visited and not from_visited and e_from in ents:
                        jp = [[e_to, to_att], [e_from, from_att]]
                        external_entity, external_attr = e_to, to_att
                        internal_entity, internal_attr = e_from, from_att
                    elif from_visited and to_visited and (e_from in ents or e_to in ents):
                        if e_from in ents:
                            jp = [[e_from, from_att], [e_to, to_att]]
                            internal_entity, internal_attr = e_from, from_att
                            external_entity, external_attr = e_to, to_att
                        else:
                            jp = [[e_to, to_att], [e_from, from_att]]
                            internal_entity, internal_attr = e_to, to_att
                            external_entity, external_attr = e_from, from_att
                    else:
                        continue

                    coll_ents = ents
                    new_visited = state["visited"] | coll_ents

                    # Assign predicates to the chosen collection and disambiguate.
                    new_sp, new_remaining_pred = [], []
                    for p in state["remaining_pred"]:
                        (new_sp if p[0] in coll_ents else new_remaining_pred).append(p)
                    new_sp = _annotate_predicates_for_collection(domain, collections[coll_candidate], new_sp, rels_list)

                    # If the internal entity appears multiple times, branch per occurrence.
                    occurrences = _collect_entity_parent_rels(collections[coll_candidate], domain["relationships"]).get(internal_entity, [])
                    if not occurrences:
                        occurrences = [None]

                    for rel in occurrences:
                        if rel is not None or len(occurrences) > 1:
                            jp = [[external_entity, external_attr], [internal_entity, internal_attr, rel]]
                        else:
                            jp = [[external_entity, external_attr], [internal_entity, internal_attr]]

                        new_ap = {
                            "c": coll_candidate,
                            "r": rname_attach,
                            "sp": new_sp,
                            "jp": jp
                        }

                        new_pending = pending[:idx_attach] + pending[idx_attach+1:]
                        next_active.append({
                            "visited": new_visited,
                            "aps": state["aps"] + [new_ap],
                            "remaining_pred": new_remaining_pred,
                            "pending_rels": new_pending
                        })

            active = next_active

        plans.extend(plans_for_start)

    qp_dict = {}
    for i, aps in enumerate(plans, start=1):
        qp_name = f"qp{i}_{query_name.lower()}"
        qp_dict[qp_name] = {
            "freq": query["f"],
            "type": query["type"],
            "aps": aps
        }

    return qp_dict


def generate_query_plans(domain, collections, queries):
    """sì
    queries: dict del tipo {"Q1": {...}, "Q2": {...}, ...}
    """
    all_plans = {}
    for qname, q in queries.items():
        #se è di tipo write usa la funzione apposita
        if q['type']=='w':
            plans_for_q = generate_write_query_plans_from_query(domain, collections, qname, q)
        else:
            plans_for_q = generate_read_query_plans(domain, collections, qname, q)
        
        #print(f"Generated {len(plans_for_q)} plans for query {qname}.")
        all_plans.update(plans_for_q)
    return all_plans


if __name__ == "__main__" :

    inputfile = "json/rubis_c1.json"
    domainfile = "json/domain_rubis.json"
    workload_file = "json/workload_Rubis.json"
    workload= load_workload(workload_file)
    queries=workload['workload']
    domain = load_domain(domainfile)
    collections, indexes, qp1 = load_solution(inputfile)
    

    


    plans = generate_query_plans(domain, collections, queries)
    print("Generated", len(plans), "query plans.")
    for qp_name, qp in plans.items():
        print("\nQuery Plan:", qp_name)
        print(qp)    
 
 
