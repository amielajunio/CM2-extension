# costModel

Toolkit per generare piani di query, derivare indici, stimare cardinalita/spazio e costruire grafi delle query a partire da domini JSON.

## Prerequisiti
- Python 3.10+
- `networkx` installato nell'ambiente attivo.

## File principali
- `loadJSON.py`: loader/serializer di dominio, collezioni, indici, query plan e workload.
- `plansUtils.py`: generatori di piani (versione classica e sequenziale) e supporto per query write.
- `workloadUtils.py`: converte le query in grafi/alberi (signature) e unisce le signature con la stessa root; stampa anche il formato stringa tipo `[Cu < Or < (Cr, Py)]`.
- `indexUtils.py`: calcolo degli indici richiesti e confronto con quelli esistenti.
- `utils.py`: funzioni di cardinalita, spazio documenti/indici e supporto costi.
- `format_output.py`: formatta e parse collection signature da/verso stringhe compatte.
- `json/`: domini, collezioni/indici, workload, parametri fisici.

## Formati dati (post parsing)
- Dominio: `entities` con attributi e cardinalita; `relationships` con estremi, attributi di join, avg/max card.
- Collections: alberi `{root: {child: {...}}}` preservati dal loader.
- Indexes: `indexes[name]["IX"]` lista di indici, ciascuno lista di coppie `(ent, attr)`.
- Query plans: `qps[name] = {"freq": f, "type": "r|w", "aps": [{"c": collection_tree|nome_coll, "r": rel|None, "sp": [...], "jp": [...]]}]}`. In `jp`, the internal-side reference can include a 3rd field (provenance relation) to disambiguate duplicate entities inside the same collection; in that case the planner may branch and generate multiple alternative read plans.
- Workload: `{"workload": {"Qx": {"f":..., "type":..., "entities": [...], "rels": [...], "pred": [[ent,att,val],...]}}}`. Il pred puo includere un 4° campo opzionale con la relazione di provenienza (es. `["It","idI",1,"gets"]`) per disambiguare entita duplicate nella stessa collection; `filterInst` usa automaticamente la variante con disambiguazione quando il 4° campo e presente.
- Config: `json/config.json` con parametri fisici/costi.

## Flussi d'uso
- Piani multi-query: `plansUtils.generate_query_plans(domain, collections, workload)` (sceglie il planner in base a `type`).
- Planner sequenziale singola query: `plansUtils.generate_query_plans_for_query_rel(domain, collections, qname, query)` percorre le relazioni in ordine, scegliendo la prima relazione attaccabile; se non interna, aggiunge un AP esplicito riusando le collection.
- Indici: `indexUtils.compute_required_indexes` + `normalize_existing_indexes` + `compare_indexes`.
- Spazio/costi: `utils.getDocSpace` / `getIxSpace` e `costNumDocs` per stimare cardinalita documento/indice (usa `pm` da config).
- Spazio/costi capped: `utils.cappedCostNumDocs(domain, qp, K)` calcola metriche separate (`n_cap_ix`, `n_cap_doc`, `n_cap_res`) simulando esecuzione in piu step con vincolo Firestore-style `prod(sp[2]) <= K` per ogni batch.
- Grafi e merge per root: `workloadUtils.workload_to_merged_signatures(domain, workload)` restituisce signature per root e le unificate; CLI sotto.

## Capping Predicati Composti (Firestore-style)
- `utils.cappedCostNumDocs` e allineata al vincolo Firestore sulle combinazioni finali (DNF disjunctions).
- Razionale: nei DBMS con limiti su query OR (es. Firestore Standard edition), il vincolo si applica al numero totale di disgiunzioni/combinazioni finali, non al singolo predicato isolato.
- Interpretazione nel modello: dato un AP con gruppi OR (ad esempio `in`), il numero di combinazioni e la cardinalita in DNF:
  `comb = prod_i(cardinalita_gruppo_i)`.
- Regola di capping: se `comb > K`, la query va suddivisa in piu step; ogni step contiene al piu `K` combinazioni finali.
- Strategia implementata: split ricorsivo dei predicati (solo quando necessario) fino a ottenere batch validi con `prod(sp[2]) <= K`.
- Nota: la strategia garantisce il vincolo per batch; il numero di step puo essere superiore al minimo teorico `ceil(comb/K)`.
- Effetto atteso nel cost model:
  - `n_cap_res` resta coerente con il risultato logico complessivo (unione degli step).
  - `n_cap_doc` puo crescere per effetto della riesecuzione multi-step.
- Riferimenti ufficiali Firestore:
  - Query limitations (`or`, `in`, `array-contains-any`) e limite a 30 disgiunzioni in DNF:
    https://firebase.google.com/docs/firestore/query-data/queries
  - Esempio documentato: `AND` di piu gruppi `in` che supera 30 disgiunzioni (es. 5x10=50) genera errore.
  - Nota correlata: `in` supporta fino a 30 valori di confronto sullo stesso campo.

## Esempi rapidi
Generare piani con planner classico:
```python
from loadJSON import load_domain, load_solution, load_workload
from plansUtils import generate_query_plans

domain = load_domain("json/domain_rubis_new.json")
collections, _, _ = load_solution("json/Rubis_c1.json")
workload = load_workload("json/workload_Rubis_prova.json")["workload"]
plans = generate_query_plans(domain, collections, workload)
print(plans.keys())
```

Planner sequenziale su una query:
```python
from loadJSON import load_domain, load_solution, load_workload
from plansUtils import generate_query_plans_for_query_rel
domain = load_domain("json/domain_rubis_new.json")
collections, _, _ = load_solution("json/Rubis_c1.json")
q = load_workload("json/workload_Rubis_prova.json")["workload"]["Q2"]
print(generate_query_plans_for_query_rel(domain, collections, "Q2", q))
```

Merge signature del workload e stampa stringhe:
```bash
python workloadUtils.py --domain json/domain_rubis_new.json --workload json/workload_Rubis_prova.json
```

## Note
- Le collection sono preservate come alberi annidati; il planner sequenziale non si blocca se la prima relazione non è agganciabile, ma cerca la prima relazione attaccabile mantenendo l'ordine relativo.
