import argparse
import networkx as nx
from format_output import format_collections, format_collection_tree
from loadJSON import load_workload, load_domain
from utils import derive_tree_from_graph, derive_tree_signature


def get_root_for_query(query, domain):
    """
    Sceglie una root per la query:
    - entità del primo predicato, se presente
    - altrimenti prima entità dichiarata
    - altrimenti entità 'from' della prima relazione
    - altrimenti None
    """
    if query.get("pred"):
        return query["pred"][0][0]
    if query.get("entities"):
        return query["entities"][0]
    if query.get("rels"):
        r0 = domain["relationships"][query["rels"][0]]
        return r0["from"]
    return None


def build_query_graph(domain, rels):
    """
    Costruisce un grafo non orientato limitato alle relazioni della query.
    """
    G = nx.Graph()
    for rname in rels:
        r = domain["relationships"][rname]
        G.add_edge(r["from"], r["to"], label=rname, direction=(r["from"], r["to"]))
    return G


def signature_from_query(domain, query):
    """
    Converte una query in un albero (signature) simile a una collection,
    usando come root la scelta di get_root_for_query.
    """
    rels = query.get("rels", [])
    root = get_root_for_query(query, domain)

    # nodi coinvolti: endpoint delle relazioni + entità dichiarate
    rel_nodes = set()
    for rname in rels:
        r = domain["relationships"][rname]
        rel_nodes.update([r["from"], r["to"]])
    all_nodes = set(query.get("entities", [])) | rel_nodes
    if root is None and all_nodes:
        root = next(iter(all_nodes))

    if not all_nodes:
        return {}
    if len(all_nodes) == 1:
        return {root: {}}

    G = build_query_graph(domain, rels)
    # assicurati che tutti i nodi siano presenti anche se isolati
    for n in all_nodes:
        G.add_node(n)

    tree = derive_tree_from_graph(G, root, list(all_nodes))
    return derive_tree_signature(tree)


def merge_signatures(sig_list):
    """
    Unisce più signature (alberi annidati) con la stessa root.
    """
    if not sig_list:
        return {}
    merged = {}
    for sig in sig_list:
        if not sig:
            continue
        root, children = next(iter(sig.items()))
        if root not in merged:
            merged[root] = {}
        merged[root] = _merge_children(merged[root], children)
    return merged


def _merge_children(base, new):
    """
    Unione ricorsiva dei figli di due signature (dizionari annidati).
    """
    for child, sub in new.items():
        if child in base:
            base[child] = _merge_children(base[child], sub)
        else:
            base[child] = sub
    return base

def _entity_in_signature(entity, sig):
    """
    Controlla ricorsivamente se un'entità è presente in una signature.
    """
    if entity in sig:
        return True
    for child, sub in sig.items():
        if _entity_in_signature(entity, sub):
            return True
    return False

def workload_to_merged_signatures(domain, workload):
    """
    Raggruppa le query per root e unisce le signature di quelle con la stessa root.
    Ritorna:
      - merged: dict root -> signature unificata
      - grouped: dict root -> lista di signature (una per query)
    """
    grouped = {}
    for qname, q in workload.items():
        #solo se query di lettura
        if q.get("type","r") != "r":
            continue
        sig = signature_from_query(domain, q)
        if not sig:
            continue
        root = next(iter(sig.keys()))
        grouped.setdefault(root, []).append(sig)

    merged = {root: merge_signatures(sig_list) for root, sig_list in grouped.items()}
    #se alcune entità non sono coperte dalle relazioni, mantienile come nodi singoli
    for en in domain["entities"].keys():
        found = False
        for root, sig in merged.items():
            if en == root or _entity_in_signature(en, sig):
                found = True
                break
        if not found:
            #entità non coperta, aggiungila come signature a sé stante
            merged[en] = {en: {}}
            grouped.setdefault(en, []).append({en: {}})


    return merged, grouped


# ...existing code...
def main():
    parser = argparse.ArgumentParser(
        description="Genera e unisce i grafi (signature) delle query di un workload per root."
    )
    parser.add_argument(
        "--domain", default="json/domain.json", help="File dominio JSON"
    )
    parser.add_argument(
        "--workload", default="json/workloadChenSimple.json", help="File workload JSON"
    )
    args = parser.parse_args()

    domain = load_domain(args.domain)
    workload = load_workload(args.workload)["workload"]

    merged, grouped = workload_to_merged_signatures(domain, workload)

    print("Signature per root prima del merge (formato stringa):")
    for root, sig_list in grouped.items():
        print(f"- root {root}: {format_collections(sig_list)}")

    print("\nSignature unificate per root (formato stringa):")
    for root, sig in merged.items():
        print(f"- root {root}: [{format_collection_tree(sig)}]")
    #salva in fomato solution JSON
    from loadJSON import save_solution
    save_solution("json/genChen.json", merged, {}, {})

if __name__ == "__main__":
    main()
