import csv
import json
from copy import deepcopy

from loadJSON import load_domain, load_solution
from utils import costNumDocsKapped, getDocSpace, getIxSpace


BIL = 1_000_000_000


def load_pm(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)["pm"]


def collection_doc_space_bytes(domain, pm, cname, ctree):
    # getDocSpace returns a tuple where the first item is total collection bytes.
    return getDocSpace(domain, pm, (cname, ctree), None)[0]


def collection_ix_space_bytes(domain, pm, cname, ctree, indexes):
    return getIxSpace(domain, pm, (cname, ctree), indexes)


def build_storage_rows(domain, pm, collections, indexes):
    rows = []
    total_doc_bytes = 0
    total_ix_bytes = 0

    for cname, ctree in collections.items():
        doc_bytes = collection_doc_space_bytes(domain, pm, cname, ctree)
        ix_bytes = collection_ix_space_bytes(domain, pm, cname, ctree, indexes)
        doc_cost = doc_bytes * pm["usd_s"] / BIL
        ix_cost = ix_bytes * pm["usd_s"] / BIL

        total_doc_bytes += doc_bytes
        total_ix_bytes += ix_bytes

        rows.append({
            "row_type": "storage_collection",
            "name": cname,
            "doc_bytes": doc_bytes,
            "doc_cost_usd": doc_cost,
            "ix_bytes": ix_bytes,
            "ix_cost_usd": ix_cost,
            "total_bytes": doc_bytes + ix_bytes,
            "total_cost_usd": doc_cost + ix_cost,
            "n_doc": "",
            "n_ix": "",
            "n_doc_k": "",
            "n_ix_k": "",
            "n_q": "",
            "n_q_k": "",
        })

    total_doc_cost = total_doc_bytes * pm["usd_s"] / BIL
    total_ix_cost = total_ix_bytes * pm["usd_s"] / BIL
    total_bytes = total_doc_bytes + total_ix_bytes
    total_cost = total_doc_cost + total_ix_cost

    rows.append({
        "row_type": "storage_total",
        "name": "ALL",
        "doc_bytes": total_doc_bytes,
        "doc_cost_usd": total_doc_cost,
        "ix_bytes": total_ix_bytes,
        "ix_cost_usd": total_ix_cost,
        "total_bytes": total_bytes,
        "total_cost_usd": total_cost,
        "n_doc": "",
        "n_ix": "",
        "n_doc_k": "",
        "n_ix_k": "",
        "n_q": "",
        "n_q_k": "",
    })

    return rows


def build_qp_rows(domain, collections, qps, k):
    rows = []
    for qp_name, qp_in in qps.items():
        qp = deepcopy(qp_in)
        for ap in qp["aps"]:
            ap["c"] = collections[ap["c"]]

        costNumDocsKapped(domain, qp, k)

        rows.append({
            "row_type": "query_plan",
            "name": qp_name,
            "doc_bytes": "",
            "doc_cost_usd": "",
            "ix_bytes": "",
            "ix_cost_usd": "",
            "total_bytes": "",
            "total_cost_usd": "",
            "n_doc": qp["n_doc"],
            "n_ix": qp["n_ix"],
            "n_doc_k": qp["n_doc_k"],
            "n_ix_k": qp["n_ix_k"],
            "n_q": qp["n_q"],
            "n_q_k": qp["n_q_k"],
        })

    return rows


def write_csv(rows, out_csv):
    fields = [
        "row_type",
        "name",
        "doc_bytes",
        "doc_cost_usd",
        "ix_bytes",
        "ix_cost_usd",
        "total_bytes",
        "total_cost_usd",
        "n_doc",
        "n_ix",
        "n_doc_k",
        "n_ix_k",
        "n_q",
        "n_q_k",
    ]

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def exec_main():
    domain = load_domain("json/domain.json")
    collections, indexes, qps = load_solution("json/Alpha_test2.json")
    pm = load_pm("json/config.json")
    k = 30

    rows = []
    rows.extend(build_storage_rows(domain, pm, collections, indexes))
    rows.extend(build_qp_rows(domain, collections, qps, k))

    out_csv = "json/Alpha_test2_analysis.csv"
    write_csv(rows, out_csv)
    print(f"CSV scritto in: {out_csv}")


if __name__ == "__main__":
    exec_main()
