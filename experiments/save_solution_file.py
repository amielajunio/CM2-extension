import argparse
import pprint

from loadJSON import load_domain, load_solution, load_workload,save_solution
from plansUtils import generate_query_plans
from indexUtils import compute_required_indexes
from utils import costNumDocs,getDocEntrySpace

pm = {
    "bool": 1,
    "str": 1,
    "date": 19,
    "int": 8,
    "float": 8,
    "docOverhead": 1250,
    "ixOverhead": 50,
    "usd_ix": 0.0003,
    "usd_doc_r": 0.125,
    "usd_doc_w": 0.625,
    "usd_s": 0.15,
    "docCompressionFactor": 0.5,
    "ixCompressionFactor": 0.5,
}



def create_solution_file(domain,workload,collections, outfile):
    
    
    #genera in automatico i qp 
    plans = generate_query_plans(domain, collections, workload)
    #calcolo i costi di storage delle collezioni
    storage_costs = {}
    for c in collections.items():
        name = c[0]
        ds = getDocEntrySpace(domain, pm, c)
        storage_costs[name] = ds
        print(f'Collection {name}: doc entry size = {round(ds,2)} bytes')

    #calcolo i costi di tutti i piani 
    for name in plans.keys():
        print(f'Piano {name}:')
        qp = plans[name]
        #stampa il piano in linea
        print(f'\t{qp["aps"]}')


        for ap in qp['aps']:
            #prima di passare gli aps a utils devo sostituire il nome della con la loro struttura
            ap['original_c'] = ap['c']
            ap['c'] = collections[ap['original_c']]
            #aggiunge lo storage cost della collection al qp
            ap['storage_cost'] = storage_costs[ap['original_c']]
        #costi dei piani    
        costNumDocs(domain,qp)

        #moltiplica il numero dei documenti per la loro dimensione
        qp['c_doc'] = 0
        for ap in qp['aps']:
            qp['c_doc'] += ap['n_doc'] * ap['storage_cost']
            #dopo devo risostituire il nome della collection
            ap['c'] = ap['original_c']
            del ap['original_c']
        
        print(f'\t{round(qp["n_doc"],2)}\t{round(qp["n_ix"],2)}\t{round(qp["c_doc"]/1000000,2)} MB')
        

    #se una query ha 2 o più qp sceglie quello che costa meno in termini di n_doc * sizeDoc
    final_plans = {}
    for name in plans.keys():
        #il nome della query è la seconda parte del nome del piano
        query_name = name.split('_')[1]
        qp = plans[name]
        if query_name not in final_plans:
            final_plans[query_name] = qp
        else:
            if qp['c_doc'] < final_plans[query_name]['c_doc']:
                final_plans[query_name] = qp    

    #ora che ho il qps completo calcolo gli indici necessari
    required = compute_required_indexes(collections, final_plans)       

    #salvo in output i piani finali e gli indici richiesti da Budgeteer
    
    save_solution(outfile, collections, required, final_plans)
    #il file di output è un csv
    filename = outfile.replace('.json', '_costs.txt')
    #in un file separato salvo le stime di docs e indici per ogni piano
    with open(filename, "w", encoding="utf-8") as f:
        f.write('Plan, EDoc, EIdx\n')
        for name in final_plans.keys():
            qp = final_plans[name]
            f.write(f'{name}, {round(qp["n_doc"],3)}, {round(qp["n_ix"],3)}\n')
            
    #print('\nRequired Indexes and Storage Costs:')
    #for coll in sorted(required.keys()):


          
###############


def exec_main():    
    """
    #lettura file di dominio, collezioni e workload e collezioni
    domain = load_domain("json/domain_Rubis.json")
    workload = load_workload("json/workload_RubisPaperB.json")["workload"]
    collections, _,_ = load_solution("json/Rubis_c1.json")
    outfile="json/RubisPaperAlpha.json"
    """
    #lettura file di dominio, collezioni e workload e collezioni
    domain = load_domain("json/domain.json")
    workload = load_workload("json/workloadChen.json")["workload"]
    collections, _,_ = load_solution("json/Alpha.json")
    outfile="json/Alpha_test1.json"
    
    create_solution_file(domain,workload,collections, outfile)

def exec_1test():    
    
    #lettura file di dominio, collezioni e workload e collezioni
    domain = load_domain("json/domain_Rubis.json")
    workload = load_workload("json/workload_RubisPaper_Q8.json")["workload"]
    coll_name = "Rubis_p1"
    collections, _,_ = load_solution(f"json/{coll_name}.json")
    outfile = f"json/{coll_name}_Q8_out.json"
    
    create_solution_file(domain,workload,collections, outfile)

def exec_manyRubisCustom():
    collections_files = [
        "json/Rubis_c1.json","json/Rubis_c2a.json", "json/Rubis_c2b.json" 
    ]
    domain = load_domain("json/domain_Rubis.json")
    workload = load_workload("json/workload_Rubis.json")["workload"]
    
    for coll_file in collections_files:
        collections, _,_ = load_solution(coll_file)
        coll_name = coll_file.split('/')[-1].replace('.json','')
        outfile = f"json/{coll_name}_out.json"
    
        create_solution_file(domain,workload,collections, outfile)





def exec_manyRubisPaper():
    collections_files = [
        "json/Rubis_p1.json","json/Rubis_p2a.json", "json/Rubis_p2b.json" 
    ]
    domain = load_domain("json/domain_Rubis.json")
    workload = load_workload("json/workload_RubisPaperB.json")["workload"]
    
    for coll_file in collections_files:
        collections, _,_ = load_solution(coll_file)
        coll_name = coll_file.split('/')[-1].replace('.json','')
        outfile = f"json/{coll_name}_out.json"
    
        create_solution_file(domain,workload,collections, outfile)


def exec_manyRubisPaperOrig():
    collections_files = [
        "json/RubisAlpha.json","json/RubisBeta.json", "json/RubisGamma.json" 
    ]
    domain = load_domain("json/domain_Rubis.json")
    workload = load_workload("json/workload_RubisPaperQ4.json")["workload"]
    
    for coll_file in collections_files:
        collections, _,_ = load_solution(coll_file)
        coll_name = coll_file.split('/')[-1].replace('.json','')
        outfile = f"json/{coll_name}_out.json"
    
        create_solution_file(domain,workload,collections, outfile)


if __name__ == "__main__":
    #exec_manyRubisCustom()
    #exec_manyRubisPaper()
    #exec_manyRubisPaperOrig()
    exec_main()
    #exec_1test()    

