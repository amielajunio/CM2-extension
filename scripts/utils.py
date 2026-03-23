import math
import networkx as nx

'''
PAPER FUNCTIONS
'''

#calcola la cardinalità dell'entità di cardinalità massima in una collezione
def maxCard(domain,c):
    maxCard = 0
    for e in get_entities_in_coll(c):
        #se e non è null e la cardinalità dell'entità e è maggiore di maxCard
        if e and domain["entities"][e]["card"]>maxCard:
            maxCard = domain["entities"][e]["card"]
    return maxCard

#calcola la cardinalità dell'entità di cardinalità massima in filtro
def maxCardInFilter(domain,sps):
    maxCard = 0
    for sp in sps:
        e= sp[0]
        if e and domain["entities"][e]["card"]>maxCard:
           maxCard = domain["entities"][e]["card"]
    return maxCard


def _is_int_like(x):
    return isinstance(x, int) or (isinstance(x, float) and x.is_integer())


def _sp_cardinality(sp):
    return sp[2] if len(sp) >= 3 else 1


def _split_predicate_value(sp, chunks):
    split = []
    for c in chunks:
        s = list(sp)
        s[2] = c
        split.append(tuple(s))
    return split


def _split_value_with_cap(v, cap):
    if cap <= 0:
        raise ValueError("cap must be > 0")
    if v <= cap:
        return [v]
    if _is_int_like(v):
        remaining = int(round(v))
        chunk_cap = max(1, int(math.floor(cap)))
        chunks = []
        while remaining > 0:
            chunk = min(chunk_cap, remaining)
            chunks.append(chunk)
            remaining -= chunk
        return chunks
    parts = int(math.ceil(v / cap))
    part_value = v / parts
    return [part_value for _ in range(parts)]


def _split_value_unit_like(v):
    # Fallback split used when even one value on this predicate still exceeds K due to other predicates.
    if _is_int_like(v):
        iv = int(round(v))
        if iv <= 1:
            return [v]
        return [1 for _ in range(iv)]
    parts = max(2, int(math.ceil(v)))
    part_value = v / parts
    return [part_value for _ in range(parts)]


def _expand_capped_sps(sps, K):
    # Firestore-style cap: each executable batch must satisfy product(sp[2]) <= K.
    if K is None or K < 1:
        raise ValueError("K must be >= 1")
    if not sps:
        return [[]]

    queue = [list(sps)]
    batches = []

    while queue:
        current = queue.pop()
        cards = [_sp_cardinality(sp) for sp in current]
        if any(v < 0 for v in cards):
            raise ValueError("Predicate cardinalities must be >= 0")

        combos = math.prod(cards)
        if combos <= K:
            batches.append([tuple(sp) for sp in current])
            continue

        split_candidates = [idx for idx, sp in enumerate(current) if len(sp) >= 3 and _sp_cardinality(sp) > 1]
        if not split_candidates:
            raise ValueError(f"Cannot satisfy cap K={K} for predicate set: {current}")

        split_idx = max(split_candidates, key=lambda idx: _sp_cardinality(current[idx]))
        sp_to_split = current[split_idx]
        v = _sp_cardinality(sp_to_split)
        others = combos / v if v != 0 else 0

        if others <= K:
            max_chunk = K / others if others > 0 else K
            value_chunks = _split_value_with_cap(v, max_chunk)
        else:
            value_chunks = _split_value_unit_like(v)

        for split_sp in _split_predicate_value(sp_to_split, value_chunks):
            new_batch = list(current)
            new_batch[split_idx] = split_sp
            queue.append(new_batch)

    return batches


def cappedFilterInstWithoutRel(domain,c,e,sps,K):
    # Evaluate all capped batches and aggregate their contribution.
    total = 0
    for sps_batch in _expand_capped_sps(list(sps), K):
        total += filterInstWithoutRel(domain,c,e,sps_batch)
    return total


def cappedFilterInstWithRel(domain,c,e,sps,K):
    # Same as cappedFilterInstWithoutRel, but with provenance-aware predicate disambiguation.
    total = 0
    for sps_batch in _expand_capped_sps(list(sps), K):
        total += filterInstWithRel(domain,c,e,sps_batch)
    return total


def cappedFilterInst(domain,c,e,sps,K):
    # Pick the same filtering strategy used by filterInst, then apply capped batching.
    use_rel = any(len(sp) >= 4 for sp in sps)
    return cappedFilterInstWithRel(domain,c,e,sps,K) if use_rel else cappedFilterInstWithoutRel(domain,c,e,sps,K)


def cappedGetJoinSp(domain,aps,i,isps_by_ap,K):
    # Derive the join selectivity predicate for AP i using the batch-specific indexed predicates
    # from the latest compatible previous AP.

    def getl_prev():
        l_prev = []
        for j in range(min(i,len(aps))):
            if e_join_j in get_entities_in_coll(aps[j]["c"]):
                l_prev.append(j)
        return l_prev

    api = aps[i]
    r = domain["relationships"][api["r"]]
    e_join_i = r["from"] if r["from"] in get_entities_in_coll(api["c"]) else r["to"]
    e_join_j = r["to"] if r["from"]==e_join_i else r["from"]

    l_prev = getl_prev()
    if len(l_prev)==0:
        temp = e_join_i
        e_join_i = e_join_j
        e_join_j = temp
        if e_join_i not in get_entities_in_coll(api["c"]):
            raise Exception("Error in cappedGetJoinSp: neither e_join_i nor e_join_j are in the collection of the current ap")
        l_prev = getl_prev()
        if len(l_prev)==0:
            raise Exception("Error in cappedGetJoinSp: neither e_join_i nor e_join_j are in the collections of previous aps")

    prev = max(l_prev)
    prev_isps = isps_by_ap.get(prev, [])
    # prev_isps already come from one capped batch; do not apply capping again here.
    nv = filterInst(domain,reroot(aps[prev]["c"],e_join_j,domain),e_join_j,prev_isps)
    if(e_join_j==r["to"]):
        nv = relInst(domain,nv,e_join_j,e_join_i)
    ja = getJoinAttr(e_join_i,r)
    return (e_join_i,ja,nv)


def cappedCostNumDocs(domain,qp,K):
    if K is None or K <= 0:
        raise ValueError("K must be > 0")

    for ap in qp["aps"]:
        ap["n_cap_doc"] = 0
        ap["n_cap_res"] = 0
        ap["n_cap_ix"] = 0
    qp["n_cap_doc"] = 0
    qp["n_cap_res"] = 0
    qp["n_cap_ix"] = 0

    # Each state is one branch in the capped execution tree and keeps, for every processed AP,
    # the indexed predicates used in that branch (needed to compute downstream join selectivities).
    states = [{"isps_by_ap": {}}]

    for i in range(len(qp["aps"])):
        ap = qp["aps"][i]
        root = getRootOfColl(ap["c"])
        next_states = []
        ap_n_cap_doc = 0
        ap_n_cap_res = 0
        ap_n_cap_ix = 0

        for state in states:
            # Base predicates for this AP. Write plans may have no "sp".
            sps_base = list(ap.get("sp", []))

            # For read plans, add the join predicate propagated from previous APs.
            # For write plans we keep the original behavior (no synthetic join predicate).
            if i > 0 and qp["type"] != "w":
                sps_base.append(cappedGetJoinSp(domain,qp["aps"],i,state["isps_by_ap"],K))

            if "ix" not in ap.keys():
                ap["ix"] = []
                for att in sps_base:
                    ap["ix"].append((att[0],att[1]))

            # Execute all capped batches for the current AP.
            # Each batch is built so that product(sp[2]) <= K (Firestore DNF cap).
            for sps in _expand_capped_sps(sps_base, K):
                isps = []
                for sp in sps:
                    for att in ap["ix"]:
                        if att[0]==sp[0] and att[1]==sp[1]:
                            isps.append(sp)

                maxCardIx = math.prod([getCardOfAttr(domain,sp[0],sp[1]) for sp in isps])
                n_cap_ix = math.prod([sp[2] for sp in sps]) * min(maxCardIx,maxCardInFilter(domain,sps)) / maxCardIx
                n_cap_doc = filterInst(domain,ap["c"],root,isps)
                n_cap_res = filterInst(domain,ap["c"],root,sps)

                ap_n_cap_ix += n_cap_ix
                ap_n_cap_doc += n_cap_doc
                ap_n_cap_res += n_cap_res

                # Propagate branch-specific indexed predicates to following APs.
                new_state_isps = dict(state["isps_by_ap"])
                new_state_isps[i] = list(isps)
                next_states.append({"isps_by_ap": new_state_isps})

        ap["n_cap_ix"] = ap_n_cap_ix
        ap["n_cap_doc"] = ap_n_cap_doc
        ap["n_cap_res"] = ap_n_cap_res
        qp["n_cap_ix"] += ap_n_cap_ix
        qp["n_cap_doc"] += ap_n_cap_doc
        qp["n_cap_res"] += ap_n_cap_res
        states = next_states
    return




def costNumDocs(domain,qp):
    for i in range(len(qp["aps"])):
        root = getRootOfColl(qp["aps"][i]["c"])
        sps = list(qp["aps"][i]["sp"])
        if i>0 and qp["type"] != "w": #aggiunto perchè i piani di write non hanno sp
            sps.append(getJoinSp(domain,qp["aps"],i))
        qp["aps"][i]["sps"] = sps # Added to redo filterInst with a rerooted collection at a later stage if needed

        # get indexed attributes
        if "ix" not in qp["aps"][i].keys():
            qp["aps"][i]["ix"] = []
            for att in sps:
                qp["aps"][i]["ix"].append((att[0],att[1]))
        isps = []
        for sp in sps:
            for att in qp["aps"][i]["ix"]:
                if att[0]==sp[0] and att[1]==sp[1]:
                    isps.append(sp)
        qp["aps"][i]["isps"] = isps # Added to do getJoinSp at a later stage if needed

        #if len([sp for sp in sps if sp[0]==root and sp[1]==getPkOfEntity(domain,root)])>0:
        #if len(sps)==1 and (sps[0][0]==root and sps[0][1]==getPkOfEntity(domain,root)): 
        #    qp["aps"][i]["n_ix"] = 0
        #else:
        maxCardIx = math.prod([getCardOfAttr(domain,sp[0],sp[1]) for sp in isps]) 
        
        maxEname = max(get_entities_in_coll(qp["aps"][i]["c"]), key=lambda e: domain["entities"][e]["card"])
        maxE = domain["entities"][maxEname]["card"]
        #cardIx = min(maxCardIx, maxE)
        # qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * cardIx / maxCardIx   # versione ENR: considera tutte le entity della collezione per prendere cardIx - sbaglia nel caso in cui l'entità con max card non sia coinvolta (ne direttamente, ne nel path) nei filtri

        #qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * min(maxCardIx,domain["entities"][root]["card"]) / maxCardIx           
        #qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * min(maxCardIx,maxCard(domain,qp["aps"][i]["c"])) / maxCardIx #ALE: nuova versione
        
        qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * min(maxCardIx,maxCardInFilter(domain,sps)) / maxCardIx # versione ALE: considera solo le entity coinvolte nel filtro - sbaglia nel caso in cui l'entità con max card sia coinvolta nel path (ma non direttamente) ne filtri
        #ALE: se l'entità nella root non è coinvolta nel join allora la stima potrebbe essere più bassa, ma consideriamo di leggere anche entità duplicate
        #perchè non è possibile stimare quando occorre una sola istanza o più di una e perchè i sistemi reali non permettono di selezionare i PID degli indici 
        qp["aps"][i]["n_doc"] = filterInst(domain,qp["aps"][i]["c"],root,isps)
        qp["aps"][i]["n_res"] = filterInst(domain,qp["aps"][i]["c"],root,sps)
    qp["n_ix"] = sum([qp["aps"][i]["n_ix"] for i in range(len(qp["aps"]))])
    qp["n_doc"] = sum([qp["aps"][i]["n_doc"] for i in range(len(qp["aps"]))])
    qp["n_res"] = sum([qp["aps"][i]["n_res"] for i in range(len(qp["aps"]))])
    return

def costNumDocsKapped(domain,qp,K):
    for i in range(len(qp["aps"])):
        root = getRootOfColl(qp["aps"][i]["c"])
        sps = list(qp["aps"][i]["sp"])
        if i>0 and qp["type"] != "w": #aggiunto perchè i piani di write non hanno sp
            sps.append(getJoinSp(domain,qp["aps"],i))
        qp["aps"][i]["sps"] = sps # Added to redo filterInst with a rerooted collection at a later stage if needed

        # get indexed attributes
        if "ix" not in qp["aps"][i].keys():
            qp["aps"][i]["ix"] = []
            for att in sps:
                qp["aps"][i]["ix"].append((att[0],att[1]))
        isps = []
        for sp in sps:
            for att in qp["aps"][i]["ix"]:
                if att[0]==sp[0] and att[1]==sp[1]:
                    isps.append(sp)
        qp["aps"][i]["isps"] = isps # Added to do getJoinSp at a later stage if needed

        #if len([sp for sp in sps if sp[0]==root and sp[1]==getPkOfEntity(domain,root)])>0:
        #if len(sps)==1 and (sps[0][0]==root and sps[0][1]==getPkOfEntity(domain,root)): 
        #    qp["aps"][i]["n_ix"] = 0
        #else:
        maxCardIx = math.prod([getCardOfAttr(domain,sp[0],sp[1]) for sp in isps]) 
        
        maxEname = max(get_entities_in_coll(qp["aps"][i]["c"]), key=lambda e: domain["entities"][e]["card"])
        maxE = domain["entities"][maxEname]["card"]
        #cardIx = min(maxCardIx, maxE)
        # qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * cardIx / maxCardIx   # versione ENR: considera tutte le entity della collezione per prendere cardIx - sbaglia nel caso in cui l'entità con max card non sia coinvolta (ne direttamente, ne nel path) nei filtri

        #qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * min(maxCardIx,domain["entities"][root]["card"]) / maxCardIx           
        #qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * min(maxCardIx,domain["entities"][root]["card"]) / maxCardIx           
        #qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * min(maxCardIx,maxCard(domain,qp["aps"][i]["c"])) / maxCardIx #ALE: nuova versione
        qp["aps"][i]["n_fv"] = math.prod([sp[2] for sp in sps]) # numero di combinazioni di valori dei predicati
        #se è kapped deve spezzare la query in più query con un subset dei predicati in modo che il numero di combinazioni di valori dei predicati sia minore o uguale a K
        if qp["aps"][i]["n_fv"] > K:
            #devo filtrare separatamente e sommare i risultati
            #da capire come applicare filterInst in questo caso, se applicarlo sui singoli predicati o su tutti i predicati insieme (con un subset dei predicati) - per ora applico filterInst su tutti i predicati insieme con un subset dei predicati
            
            
            #perchè non è possibile stimare quando occorre una sola istanza o più di una e perchè i sistemi reali non permettono di selezionare i PID degli indici 
            qp["aps"][i]["n_doc"] = filterInst(domain,qp["aps"][i]["c"],root,isps)
            qp["aps"][i]["n_res"] = filterInst(domain,qp["aps"][i]["c"],root,sps)

        else:     
            
            #perchè non è possibile stimare quando occorre una sola istanza o più di una e perchè i sistemi reali non permettono di selezionare i PID degli indici 
            qp["aps"][i]["n_doc"] = filterInst(domain,qp["aps"][i]["c"],root,isps)
            qp["aps"][i]["n_res"] = filterInst(domain,qp["aps"][i]["c"],root,sps)
        #i dati dell'indice non dipendono dal Kap perchè si suppone che l'indice venga usato comunque, ma se n_fv è maggiore di K allora non è possibile sfruttare appieno l'indice e quindi si considera solo la parte di indice che viene usata (quella che filtra fino a K combinazioni di valori dei predicati)    
        #ALE: se l'entità nella root non è coinvolta nel join allora la stima potrebbe essere più bassa, ma consideriamo di leggere anche entità duplicate
        qp["aps"][i]["n_ix"] = math.prod([sp[2] for sp in sps]) * min(maxCardIx,maxCardInFilter(domain,sps)) / maxCardIx # versione ALE: considera solo le entity coinvolte nel filtro - sbaglia nel caso in cui l'entità con max card sia coinvolta nel path (ma non direttamente) ne filtri
                
    qp["n_ix"] = sum([qp["aps"][i]["n_ix"] for i in range(len(qp["aps"]))])
    qp["n_doc"] = sum([qp["aps"][i]["n_doc"] for i in range(len(qp["aps"]))])
    qp["n_res"] = sum([qp["aps"][i]["n_res"] for i in range(len(qp["aps"]))])
    return



def relInst(domain,n_orig, e_orig, e_dest):
    n_dest = n_orig
    for (r,dir) in find_relationship_path(domain, e_orig, e_dest):
        if dir=="to-many":
            #n_dest = math.ceil(n_dest * domain["relationships"][r]["avgCard"]) # ceil version
            n_dest = (n_dest * domain["relationships"][r]["avgCard"]) # plain avgCard version
            # n_dest = (n_dest * round(domain["relationships"][r]["avgCard"])) # rounded avgCard version
            # n_dest = math.ceil(n_dest * round(domain["relationships"][r]["avgCard"])) # rounded ceil avgCard version
        else:
            e_par = domain["relationships"][r]["from"]
            n_dest = Cardenas(domain,n_dest, e_par)
            
    return n_dest



def getJoinSp(domain,qp,i):

    def getl_prev():
        l_prev = []
        for j in range(min(i,len(qp))):
            if e_join_j in get_entities_in_coll(qp[j]["c"]):
                l_prev.append(j)
        return l_prev        


    #if i==0 :
    #    return []
    api = qp[i]
    r = domain["relationships"][api["r"]]
    e_join_i = r["from"] if r["from"] in get_entities_in_coll(api["c"]) else r["to"]
    e_join_j = r["to"] if r["from"]==e_join_i else r["from"]
    
    #EDIT ALE 27/01/2026
    l_prev = getl_prev()
    #se l_prev è vuota significa che non ci sono ap che contengono e_join_j in questo caso devo scambiare e_join_i con e_join_j
    if len(l_prev)==0:
        #scambio e_join_i con e_join_j
        temp = e_join_i
        e_join_i = e_join_j
        e_join_j = temp
        if e_join_i not in get_entities_in_coll(api["c"]):
            raise Exception("Error in getJoinSp: neither e_join_i nor e_join_j are in the collection of the current ap")
        l_prev = getl_prev()
        if len(l_prev)==0:
            raise Exception("Error in getJoinSp: neither e_join_i nor e_join_j are in the collections of previous aps")
    

    prev = max(l_prev)
    nv = filterInst(domain,reroot(qp[prev]["c"],e_join_j,domain),e_join_j,qp[prev]["isps"]) # prendo la cardinalità dell'entità di e_join_j in funzione dei filtri che sono già stati applicati
    if(e_join_j==r["to"]):
        nv = relInst(domain,nv,e_join_j,e_join_i) # poi propago la cardinalità fino a e_join_i
    ja = getJoinAttr(e_join_i,r)
    return (e_join_i,ja,nv)

def _get_rel_between(domain, parent, child):
    candidates = []
    for r, info in domain["relationships"].items():
        if (info["from"] == parent and info["to"] == child) or (info["from"] == child and info["to"] == parent):
            candidates.append(r)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        return None
    raise ValueError(f"Ambiguous relationship between {parent} and {child}: {candidates}")


def _iter_children(subtree):
    if isinstance(subtree, dict):
        for k, v in subtree.items():
            yield k, v
    elif isinstance(subtree, (set, list, tuple)):
        for el in subtree:
            if isinstance(el, dict):
                for k, v in el.items():
                    yield k, v
            else:
                yield el, None


def filterInstWithoutRel(domain,c,e,sps):
    n_inst = domain["entities"][e]["card"]
    if get_children_of_e_in_c(e,c):
        for e_nested in get_children_of_e_in_c(e,c):
            nestedFilterInst = filterInstWithoutRel(domain,c,e_nested,sps)
            if nestedFilterInst<domain["entities"][e_nested]["card"]:
                n_inst = n_inst * relInst(domain,nestedFilterInst,e_nested,e)/domain["entities"][e]["card"]
    for sp in sps:
        if sp[0]==e:
            n_inst = n_inst * sp[2]/domain["entities"][e]["attr"][sp[1]]["card"]
    return n_inst

#versione per gestire i cicli
def filterInstWithRel(domain,c,e,sps):
    applied_unqualified = set()  # indexes of sps applied without provenance

    def _filter_node(entity, subtree, parent_rel):
        n_inst = domain["entities"][entity]["card"]
        for child, child_sub in _iter_children(subtree):
            rel_name = _get_rel_between(domain, entity, child)
            nestedFilterInst = _filter_node(child, child_sub, rel_name)
            if nestedFilterInst < domain["entities"][child]["card"]:
                n_inst = n_inst * relInst(domain, nestedFilterInst, child, entity) / domain["entities"][entity]["card"]
        for idx, sp in enumerate(sps):
            if sp[0] != entity:
                continue
            if len(sp) >= 4:
                if parent_rel is None or sp[3] != parent_rel:
                    continue
            else:
                if idx in applied_unqualified:
                    continue
                applied_unqualified.add(idx)
            n_inst = n_inst * sp[2] / domain["entities"][entity]["attr"][sp[1]]["card"]
        return n_inst

    if isinstance(c, dict):
        if e in c:
            return _filter_node(e, c[e], None)
        total = 0
        def _scan(node):
            nonlocal total
            if isinstance(node, dict):
                for k, v in node.items():
                    if k == e:
                        total += _filter_node(k, v, None)
                    _scan(v)
            elif isinstance(node, (set, list, tuple)):
                for el in node:
                    _scan(el)
        _scan(c)
        return total
    elif isinstance(c, (set, list, tuple)):
        total = 0
        for el in c:
            if isinstance(el, dict):
                for k, v in el.items():
                    if k == e:
                        total += _filter_node(k, v, None)
            else:
                if el == e:
                    total += _filter_node(e, None, None)
        return total
    else:
        return _filter_node(e, None, None)


def filterInst(domain,c,e,sps):
    # Use disambiguation only when a predicate carries provenance.
    use_rel = any(len(sp) >= 4 for sp in sps)
    #return filterInstWithoutRel(domain,c,e,sps)
    return filterInstWithRel(domain,c,e,sps) if use_rel else filterInstWithoutRel(domain,c,e,sps)



'''
UTILS
'''

# Cardenas funtion
def cardenas_function(n1,n2):
    return round(n2*(1-(1-1/n2)**n1),5)

# Call to Cardenas' function according with paper
def Cardenas(domain,n,e):
    card = domain["entities"][e]["card"]
    #return math.ceil(cardenas_function(n,card))
    return cardenas_function(n,card)

# Returns the name of the attribute that is the join attribute of a relationship
def getJoinAttr(e,r):
    return r["fromAtt"] if r["from"]==e else r["toAtt"]

# Returns the name of the primary key of an entity
def getPkOfEntity(domain,e):
    for attKey in domain["entities"][e]["attr"]:
        att = domain["entities"][e]["attr"][attKey]
        if att["pk"]:
            return attKey
    return None

# Returns the name of the primary entity of a collection
def getRootOfColl(c):
    if isinstance(c, dict):
        return list(c.keys())[0]
    elif isinstance(c, set):
        return list(c)[0]
    else:
        raise TypeError("Input must be a dictionary or a set")

# Utility function to navigate the tree of relationships
def build_graph(domain):
    G = nx.Graph()  # Change to undirected graph to allow bidirectional navigation
    
    for rel, details in domain["relationships"].items():
        G.add_edge(details["from"], details["to"], label=rel, direction=(details["from"], details["to"]))
    
    return G

# Returns the (unique) path between two entities (start and end)
# It returns tuples in the form (rel, label) where rel is the relationship and label is "to-one" or "to-many"
def find_relationship_path(domain, start, end):
    G = build_graph(domain)
    path = nx.shortest_path(G, source=start, target=end)
    
    relationships = []
    for i in range(len(path) - 1):
        rel = G[path[i]][path[i + 1]]['label']
        correct_direction = (path[i], path[i + 1]) == G[path[i]][path[i + 1]]['direction']
        relationships.append((rel, "to-many" if correct_direction else "to-one"))
    
    return relationships
# Example usage:
# start_entity = "Cu"
# end_entity = "Py"
# print(find_relationship_path(domain, start_entity, end_entity))

# Returns the set of entity names in a collection (recursive function)
def get_entities_in_coll(c, keys_set=None):
    if keys_set is None:
        keys_set = set()
    
    if isinstance(c, set):
        keys_set.update(c)
    elif isinstance(c, dict):
        for key, value in c.items():
            keys_set.add(key)
            if isinstance(value, dict):
                get_entities_in_coll(value, keys_set)
            else:
                if isinstance(value, set):
                    keys_set.update(value)
                else:
                    keys_set.add(value)
    else:
        keys_set.add(c)
    
    returned_key_set = { k for k in keys_set if k is not None }
    return returned_key_set
# Example usage
# c = {"Ct": {"Pr": {"It", "Su"}}}
# all_keys_and_leaves = get_entities_in_coll(c)
# print(all_keys_and_leaves)  # Output: {'Ct', 'Pr', 'It', 'Su'}

# Returns the set of entity names of the DIRECT CHILDREN of e in a collection (recursive function)
def get_children_of_e_in_c(e, c):
    if e in c:
        return set(c[e].keys()) if isinstance(c[e], dict) else c[e]
    for key, value in c.items():
        if isinstance(value, dict):
            result = get_children_of_e_in_c(e, value)
            if result is not None:
                return result
    return None




def derive_tree_from_graph(graph, root, node_list):
    """
    Derives a tree from the given graph with the specified root and nodes.

    Parameters:
        graph (nx.Graph): The input graph.
        root (str): The name of the root node.
        node_list (list): List of node names to include in the tree.

    Returns:
        nx.DiGraph: A directed tree derived from the graph.
    """
    # Create a subgraph with only the specified nodes
    subgraph = graph.subgraph(node_list)
    
    # Initialize an empty directed graph for the tree
    tree = nx.DiGraph()
    
    # Perform BFS from the root node
    for parent, child in nx.bfs_edges(subgraph, root):
        tree.add_edge(parent, child)
    
    return tree

def extract_node_names(tree_signature):
    """
    Extracts all node names from a tree signature.

    Parameters:
        tree_signature (dict): The tree signature in nested dictionary format.

    Returns:
        list: A list of all node names in the tree.
    """
    nodes = []

    def traverse(tree):
        if isinstance(tree, dict):
            for key, value in tree.items():
                nodes.append(key)  # Add the key (node name)
                traverse(value)    # Recursively traverse the value
        elif isinstance(tree, set):
            nodes.extend(tree)  # Add all elements in the set

    traverse(tree_signature)
    return nodes



def derive_tree_signature(tree):
    def find_root(tree):
        """
        Finds the root of a directed tree.

        Parameters:
            tree (nx.DiGraph): The input directed tree.

        Returns:
            str: The root node of the tree.
        """
        for node in tree.nodes:
            if tree.in_degree(node) == 0:  # No incoming edges
                return node
        return None  # No root found (e.g., cyclic graph)

    """
    Derives the tree signature from a NetworkX tree.

    Parameters:
        tree (nx.DiGraph or nx.Graph): The input tree.
        root (str): The root node of the tree.

    Returns:
        dict: The tree signature in nested dictionary format.
    """
    def build_signature(node):
        children = list(tree.successors(node))  # Get children of the node
        if not children:
            return {}  # Leaf node, return empty dictionary
        signature = {}
        for child in children:
            signature[child] = build_signature(child)
        return signature

    root = find_root(tree)
    return {root: build_signature(root)}

def reroot(coll,newRoot,domain):
    node_names = extract_node_names(coll)

    if(len(node_names) == 1):
        return coll
    else:
        tree = derive_tree_from_graph(build_graph(domain), newRoot, node_names)
        return derive_tree_signature(tree)
    
def getCardOfAttr(domain, e, a):
    return domain["entities"][e]["attr"][a]["card"]


def attSize(pm, a_key, a_val, n):
    return len(a_key)*pm["str"] + n*a_val["len"]*pm[a_val["type"]]

def getDocSpaceForEntity(domain, pm, c, e, n, indexes, log):
    docSize = 0
    # if(log): 
    #     print(f"getDocSpace {e} {n}")
    for a in domain["entities"][e]["attr"].keys():
        docSize += attSize(pm, a, domain["entities"][e]["attr"][a], 1)
        if(log): 
            printn = 1 if e==getRootOfColl(c[1]) else n
            print(f"{a}\t{printn}\t{len(a)*pm['str']}\t{domain['entities'][e]['attr'][a]['len']*pm[domain['entities'][e]['attr'][a]['type']]}\t1")

    if get_children_of_e_in_c(e, c[1]):
        for e_nested in get_children_of_e_in_c(e, c[1]):
            # if(log): 
            #     print(f"{e_nested} {relInst(domain, 1, e, e_nested)}")
            size = getDocSpaceForEntity(domain, pm, c, e_nested, math.ceil(relInst(domain, 1, e, e_nested)), indexes, log)
            docSize += size
            # if(log): 
            #     print(f"extend to {math.ceil(relInst(domain, 1, e, e_nested))} instances of {e_nested}: {size}")

    if e==getRootOfColl(c[1]):
        docSize += pm["docOverhead"]
        docSizeMainAttr = docSize
        if indexes is not None and c[0] in indexes:
            for na in indexes[c[0]]["nestedAttrs"]:
                a = domain["entities"][na[0]]["attr"][na[1]]
                nr = math.ceil(relInst(domain, 1, e, na[0]))
                docSize += attSize(pm, "list"+na[1], a, nr)
                if(log):
                    print(f"{'list' + na[1]}\t1\t{len('list' + na[1])}\t{(int)((attSize(pm, 'list' + na[1], a, nr) - len('list' + na[1])) / nr)}\t{nr}")
        docSizeExtraAttr = docSize - docSizeMainAttr
        if(log): 
            print(f"** Doc size {e} is {docSize} ({docSizeMainAttr}+{docSizeExtraAttr}), given {n} it totals to {docSize * n}")    
        return (docSize * n, docSizeMainAttr * n, docSizeExtraAttr * n, docSize, docSizeMainAttr, docSizeExtraAttr)
    else:
        return docSize * n


def getDocEntrySpace(domain, pm, c, indexes=[], log=False):
    cn = c[0]
    cr = getRootOfColl(c[1])
    n=domain["entities"][cr]["card"]
    size = getDocSpaceForEntity(domain, pm, c, cr, n, indexes, log)
    if(log): 
        print(f"** entry size for {c[0]} is {size/n} ({size[0]/1000000000} GB)")
    return size[0]/n


def getDocSpace(domain, pm, c, indexes, log=False):
    cn = c[0]
    cr = getRootOfColl(c[1])
    size = getDocSpaceForEntity(domain, pm, c, cr, domain["entities"][cr]["card"], indexes, log)
    if(log): 
        print(f"** Total size for {c[0]} is {size} ({size[0]/1000000000} GB)")
    return size

def getIxSpace(domain, pm, c, indexes, log=False):
    totIxSpace = 0
    if(log): 
        print(f"** COLLECTION {c[0]}")
    for ix in indexes[c[0]]["IX"]:
        if(log): 
            print(f"- Index {ix}", end="")
        ixSpace, ek, ev, sps, prodA, maxE = 0, 0, 0, [], 1, 0
        for na in ix:
            a = domain["entities"][na[0]]["attr"][na[1]]
            ek += attSize(pm, na[1], a, 1)
            sps.append((na[0], na[1], 1))
            prodA *= a["card"]
            # print(f"- {na}: attSize: {attSize(pm, na[1], a, 1)}, sp: {(na[0], na[1], 1)}, card: {a["card"]}")
        pk = domain["entities"][getRootOfColl(c[1])]["attr"][getPkOfEntity(domain,getRootOfColl(c[1]))]
        ppe = math.ceil(filterInst(domain, c[1], getRootOfColl(c[1]), sps))
        # print(f"ek: {ek}")
        # print(f"pk: {pk}, ppe: {ppe}")
        ev = pk["len"]*pm[pk["type"]] * ppe
        # print(f"ev = {pk["len"]}*{pm[pk["type"]]}*{ppe} = {ev}")
        maxEname = max(get_entities_in_coll(c[1]), key=lambda e: domain["entities"][e]["card"])
        maxE = domain["entities"][maxEname]["card"]
        # print(f"maxE: {maxE}, prodA: {prodA}")
        cardIx = min(prodA, maxE)
        # print(f"cardIx: {cardIx}")
        # print(f"entry: {ek+ev}")
        ixSpace = (pm["docOverhead"] + ek + ev) * cardIx
        # print(f"ixSpace: {ixSpace} ({ixSpace/1000000} MB)")
        totIxSpace += ixSpace
        if(log): 
            print(f", space = (overhead+ek+ev)*card = ({pm['docOverhead']}+{ek}+{ev})*{cardIx} = {ixSpace}")
    return totIxSpace

'''
MONETARY COSTS UTILS
'''

# def monetaryCostAp(domain, pm, cols, ap, fineTuning=0):
#     docSize = getDocSpace(domain, pm, ap["c"], indexes)
#     if fineTuning not in range(0,5):
#         print("Fine-tuning number not allowed")
#     elif fineTuning==0:
#         return ap["freq"] * max(pm["min_charge"][ap["type"]], cols[])



"""
def totalCosts(pm, cols, qps, ceils):
    bil = 1000000000
    mil = 1000000

    for k_qp in qps:
        costNumDocs(domain,qps[k_qp])

    l1 = 0
    for k_qp in {qp for qp in qps if qps[qp]["type"]=="r"}:
        qp = qps[k_qp]
        n_ix = qp["n_ix"] if not ceils else math.ceil(qp["n_ix"])
        n_doc = qp["n_doc"] if not ceils else math.ceil(qp["n_doc"])
        cost_ix = pm["usd_ix"]*n_ix/mil
        cost_doc = pm["usd_doc_r"]*n_doc/mil
        cost = qp["freq"] * ( cost_ix + cost_doc )
        l1 += cost
        print(f"l1: {cost}$ for ({qp['type']}), freq:{qp['freq']}, ix: {n_ix} at {pm["usd_ix"]}$/B = {cost_ix}, doc: {n_doc} at {pm["usd_doc_r"]}$/M = {cost_doc}$")
    # l2 = 0
    # for k_qp in {qp for qp in qps if qps[qp]["type"]=="w"}:
    #     qp = qps[k_qp]
    #     for ap_k in {ap for ap in range(len(qp["aps"])) if qp["aps"][ap]["n_ix"]>0}:
    #         ap = qp["aps"][ap_k]
    #         cost_ix = pm["usd_ix"]*ap["n_ix"]/bil
    #         cost_doc = pm["usd_doc_r"]*ap["n_doc"]/mil
    #         cost = qp["freq"] * ( cost_ix + cost_doc )
    #         l2 += cost
    #         print(f"l2: {cost}$ for ({qp['type']}), freq:{qp['freq']}, ix: {ap['n_ix']} at {pm["usd_ix"]}$/B = {cost_ix}, doc: {ap['n_doc']} at {pm["usd_doc_r"]}$/M = {cost_doc}$")
    l3 = 0
    for k_qp in {qp for qp in qps if qps[qp]["type"]=="w"}:
        qp = qps[k_qp]
        cost_doc = pm["usd_doc_w"]*qp["n_doc"]/mil
        cost = qp["freq"] * cost_doc
        l3 += cost
        print(f"l3: {cost}$ for ({qp['type']}), freq:{qp['freq']}, doc: {qp['n_doc']} at {pm["usd_doc_w"]}$/M = {cost_doc}$")
    l4 = 0
    for c in cols:
        cost = getDocSpaceForCol(c)*pm["usd_s"]/bil
        l4 += cost
        print(f"l4: {cost}$ for {c}")
    return l1 + l3 + l4

"""
