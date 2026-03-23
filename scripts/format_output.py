from loadJSON import load_solution

def format_collection_tree(tree):
    """
    tree: dizionario JSON di una singola collection, es:
        { "Cu": { "Or": { "Cr": None, "Py": None } } }

    Ritorna una stringa tipo:
        "Cu < Or < (Cr, Py)"
    """

    # Ogni collection ha un solo root (una sola chiave al top level)
    if not isinstance(tree, dict) or len(tree) != 1:
        raise ValueError("La collection deve avere esattamente una root.")

    def rec(node):
        """
        node: dict con un'unica chiave {entity: subtree}
              oppure None
        """
        if node is None:
            return ""

        if not isinstance(node, dict):
            raise ValueError("Nodo non valido nel tree.")

        # Estraggo la singola entità del nodo
        entity = list(node.keys())[0]
        subtree = node[entity]

        # Caso foglia
        if subtree is None or subtree == {}:
            return entity

        # Caso con figli
        children = list(subtree.items())

        # Un figlio → concatenazione lineare
        if len(children) == 1:
            child_entity, child_subtree = children[0]
            return f"{entity} < {rec({child_entity: child_subtree})}"

        # Più figli → parentesi con virgole
        formatted_children = []
        for child_entity, child_subtree in children:
            formatted_children.append(rec({child_entity: child_subtree}))

        children_str = ", ".join(formatted_children)
        return f"{entity} < ({children_str})"


    # Estrai root
    root = list(tree.keys())[0]
    return f"{rec({root: tree[root]})}"


def format_collections(collections):
    """
    collections: può essere
        - un singolo dict (una collection)
        - un dict di collections es: {"c_alpha_1": {...}, "c_alpha_2": {...}}
        - una lista di collections [ {...}, {...} ]

    Ritorna una stringa con le collecions formattate e separate da ", ".
    """

    # Caso una sola collection
    if isinstance(collections, dict) and (
          len(collections) == 1 and isinstance(list(collections.values())[0], dict)
    ):
        # input del tipo  {"Cu": {...}}  → una collection singola
        return f"[{format_collection_tree(collections)}]"

    # Caso dict di collection (es: {"c1": {...}, "c2": {...}})
    if isinstance(collections, dict):
        formatted = []
        for name, coll in collections.items():
            formatted.append(f"[{format_collection_tree(coll)}]")
        return " , ".join(formatted)

    # Caso lista di collection
    if isinstance(collections, list):
        formatted = []
        for coll in collections:
            formatted.append(f"[{format_collection_tree(coll)}]")
        return " , ".join(formatted)

    raise ValueError("Formato non riconosciuto per le collections.")

def tokenize_collection_string(s):
    """
    Trasforma una stringa tipo "[Cu < Or < (Cr, Py)]"
    in una lista di token: ['[', 'Cu', '<', 'Or', '<', '(', 'Cr', ',', 'Py', ')', ']']
    """
    tokens = []
    i = 0
    while i < len(s):
        c = s[i]
        if c.isspace():
            i += 1
        elif c in "<(),[]":
            tokens.append(c)
            i += 1
        else:
            # ident = sequenza di caratteri non speciali
            start = i
            while i < len(s) and s[i] not in " <(),[]":
                i += 1
            tokens.append(s[start:i])
    return tokens

def attach_chain(parent_tree, child_tree):
    """
    Attacca child_tree alla foglia più profonda del "chain" principale
    del parent_tree.

    Assunzione: le stringhe che arrivano qui sono state prodotte
    dal tuo formatter (es. "A < B < (C, D)"), quindi esiste un
    unico path lineare dove attachare.
    """
    key = next(iter(parent_tree))
    node = parent_tree

    # scendi lungo il path lineare finché trovi un solo figlio-dict
    while isinstance(node[key], dict) and len(node[key]) == 1:
        node = node[key]
        key = next(iter(node))

    # ora node[key] deve essere None (foglia) o un punto di attacco
    if node[key] is not None:
        raise ValueError("Struttura inaspettata: non trovo una foglia None dove attaccare il child.")
    node[key] = child_tree
    return parent_tree


def parse_collection_string(s):
    """
    Parse di UNA singola collection:
        "[Cu < Or < (Cr, Py)]"
    → {"Cu": {"Or": {"Cr": None, "Py": None}}}
    """

    tokens = tokenize_collection_string(s)
    # rimuovi eventuali [ ... ] esterne
    if tokens and tokens[0] == "[" and tokens[-1] == "]":
        tokens = tokens[1:-1]

    idx = 0
    n = len(tokens)

    def parse_expr():
        """
        Expr ::= Factor ('<' Factor)*
        """
        nonlocal idx

        tree = parse_factor()

        while idx < n and tokens[idx] == "<":
            idx += 1  # salta '<'
            child_tree = parse_factor()
            tree = attach_chain(tree, child_tree)

        return tree

    def parse_factor():
        """
        Factor ::= IDENT | '(' Expr (',' Expr)* ')'
        """
        nonlocal idx

        if idx >= n:
            raise ValueError("Fine input inattesa durante il parse.")

        tok = tokens[idx]

        # Caso IDENT: es. "Cu"
        if tok not in "(),<[]":
            idx += 1
            return {tok: None}

        # Caso parentesi: "(" Expr ("," Expr)* ")"
        if tok == "(":
            idx += 1  # salta '('
            children_dict = {}

            while True:
                child_tree = parse_expr()
                # child_tree è un dict con 1+ radici (es. "(A < B, C)")
                for k, v in child_tree.items():
                    if k in children_dict:
                        raise ValueError(f"Duplicate entity '{k}' nei figli.")
                    children_dict[k] = v

                if idx >= n:
                    raise ValueError("Manca la ')' di chiusura.")
                if tokens[idx] == ",":
                    idx += 1  # salta ','
                    continue
                elif tokens[idx] == ")":
                    idx += 1  # salta ')'
                    break
                else:
                    raise ValueError(f"Token inatteso nei figli di '()': {tokens[idx]}")

            return children_dict

        raise ValueError(f"Token inatteso in factor: {tok}")

    tree = parse_expr()

    if idx != n:
        # ci sono token residui non consumati
        raise ValueError(f"Token residui dopo il parse: {tokens[idx:]}")

    return tree

def parse_collections_string(s):
    """
    Parse di UNA O PIÙ collections in una stringa, ad es.:

        "[Cu < Or < (Cr, Py)] , [Pr < (It, Su)]"

    Restituisce una lista di dict, uno per collection, ad es.:

        [
          {"Cu": {"Or": {"Cr": None, "Py": None}}},
          {"Pr": {"It": None, "Su": None}}
        ]
    """

    tokens = tokenize_collection_string(s)
    collections_tokens = []
    current = []
    depth = 0  # profondità rispetto alle [ ]

    for tok in tokens:
        if tok == "[":
            # inizio di una nuova collection
            if depth == 0:
                current = []
            depth += 1
            current.append(tok)
        elif tok == "]":
            current.append(tok)
            depth -= 1
            if depth == 0:
                # fine di una collection
                collections_tokens.append(current)
                current = []
        else:
            # token all'interno di una collection
            if depth > 0:
                current.append(tok)
            # token fuori da [ ] (es. virgole) → ignorati

    # Converte ogni lista di token in stringa e la passa a parse_collection_string
    collections = []
    for ctokens in collections_tokens:
        coll_str = " ".join(ctokens)
        collections.append(parse_collection_string(coll_str))

    return collections


if __name__ == "__main__":
    
    # legge da file 
    collections, _, _ = load_solution("json/alpha.json")
    stringa=format_collections(collections)
    print(stringa)
    json_trees = parse_collections_string(stringa)
    for t in json_trees:
        print(t)
    
    