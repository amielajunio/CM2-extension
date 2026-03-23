#funzioni per generare la lista degli indici a partire da dominio, collezioni e query

from loadJSON import load_domain, load_solution



def _sp_key(sp_item):
    return tuple(sp_item[:4]) if len(sp_item) >= 4 else tuple(sp_item[:2])


def compute_required_indexes(collections, query_plans):
    """
    collections: dict come 'Collections' del tuo esempio
    query_plans: dict come 'Query Plans' del tuo esempio

    Ritorna:
    {
      'c_alpha_1': { (('Or','idO'),), (('Cu','country'),('Or','idO')), ... },
      'c_alpha_2': { ... },
      ...
    }
    dove ogni indice è una tupla di coppie (ent, attr) in ordine.
    """

    def find_collection_name(c_spec):
        """
        Dato il campo 'c' di un access plan, trova il nome della collection
        corrispondente in 'collections'.
        """
        for name, spec in collections.items():
            if spec == c_spec:
                return name
        return None
    
    def find_collection(c_name):
        """
        Dato il campo 'c' di un access plan, trova il nome della collection
        corrispondente in 'collections'.
        """
        for name, _ in collections.items():
            if name == c_name:
                return name
        return None

    # inizializzo un set di indici per ogni collection
    result = {name: set() for name in collections.keys()}

    for qp in query_plans.values():
        for ap in qp['aps']:
            coll = find_collection(ap['c'])
            if not coll:
                # access plan che non corrisponde a nessuna collection nota
                continue

            sp = ap.get('sp') or []   # lista di triple (ent, attr, ...)
            jp = ap.get('jp') or []   # lista di due coppie (ent, attr)

            # estraggo (ent, attr) o (ent, attr, rel) da sp, mantenendo l'ordine
            sp_pairs = [_sp_key(item) for item in sp]

            # "secondo valore di jp": la seconda coppia della lista jp, se presente
            join_second = None
            if jp and len(jp) >= 2:
                join_second = tuple(jp[1])  # (ent, attr) or (ent, attr, rel)

            idx = None
            if sp_pairs and join_second:
                # caso: c'è sia sp che jp → indice composto [sp..., jp-second]
                idx = tuple(sp_pairs + [join_second])
            elif sp_pairs:
                # solo sp → indice su tutti i filtri in sp (se >1, composto)
                idx = tuple(sp_pairs)
            elif join_second:
                # solo jp → indice sulla seconda coppia di jp
                idx = (join_second,)
            else:
                # niente sp, niente jp → nessun indice richiesto
                continue
            #se l'indice è composto lo inserisce in modo che i componenti siano in ordine alfabetico
            if len(idx) > 1:
                idx = tuple(sorted(idx))

            # uso un set per eliminare duplicati in modo naturale
            result[coll].add(idx)

    return result

def normalize_existing_indexes(indexes):
    """
    Converte la struttura 'Indexes' in un dict:
    {
      'c_alpha_1': { (('Or','idO'),), (('Cu','country'),('Or','idO')), ... },
      ...
    }
    per facilitare i confronti insiemistici.
    """
    out = {}
    for coll, data in indexes.items():
        s = set()
        for idx in data['IX']:
            #ordina gli indici composti in existing
            idx = sorted(idx)
            s.add(tuple(idx))  # ogni idx è una lista di (ent, attr) o (ent, attr, rel)
        out[coll] = s
    return out




def compare_indexes(required, existing):
    """
    required: output di compute_required_indexes
    existing: output di normalize_existing_indexes

    Ritorna per ogni collection:
      - missing_in_existing: indici richiesti dai QP ma non presenti in Indexes
      - extra_in_existing: indici presenti in Indexes ma non derivabili dai QP
      - common: indici comuni
    """
    summary = {}
    for coll in required.keys() | existing.keys():
        req = required.get(coll, set())
        ex = existing.get(coll, set())
        
        summary[coll] = {
            'missing_in_existing': sorted(req - ex),
            'extra_in_existing': sorted(ex - req),
            'common': sorted(req & ex),
        }
    return summary



if __name__ == "__main__":
    # Load domain, collections, indexes, and query plans
    domain = load_domain("json/domain.json")
    collections, indexes, qps = load_solution("json/alpha.json")


    required = compute_required_indexes(collections, qps)
    #print(required)
    existing = normalize_existing_indexes(indexes)
    report = compare_indexes(required, existing)
    #pretty print the report
    import pprint
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(report)
