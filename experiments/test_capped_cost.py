import argparse
import pprint

from loadJSON import load_domain, load_solution
from utils import costNumDocs,cappedCostNumDocs



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
    #workload = load_workload("json/workload_Chen.json")["workload"]
    collections, indexes, qps = load_solution("json/Alpha_test2.json")
    #costi dei piani    
    for qpi in qps:
        print(f'Piano {qpi}:')
        print(f'\t{qps[qpi]["aps"]}')
        qp=qps[qpi]
        for ap in qp['aps']:
            #prima di passare gli aps a utils devo sostituire il nome della con la loro struttura
            ap['original_c'] = ap['c']
            ap['c'] = collections[ap['original_c']]


        costNumDocs(domain,qp)
        cappedCostNumDocs(domain,qp,30)
        print(f'\t{round(qp["n_doc"],2)}\t{round(qp["n_ix"],2)}\t{round(qp["n_res"],2)}')
        print(f'\t{round(qp["n_cap_doc"],2)}\t{round(qp["n_cap_ix"],2)}\t{round(qp["n_cap_res"],2)} ')

    #outfile="json/Alpha_testCap.json"
    
if __name__ == "__main__":
    exec_main()
    #exec_1test()    

