"""
Incremental local → API synchroniser.

* Sends ALL .txt files under ./raw_data to `/docs/*`.
* Remembers which docs were actually added/updated.
* Sends *only those* to `/chunks/*`.
* Deletes chunks for docs that disappeared locally.
"""

import os
from typing import List, Set

import requests
from langchain_community.document_loaders import TextLoader

# ── config ────────────────────────────────────────────────────────────────────
BASE_URL = "http://127.0.0.1:8000"
RAW_FOLDER = "raw_data"
BATCH_SIZE = 50


# ── helpers ───────────────────────────────────────────────────────────────────
def normalise(fname: str) -> str:
    """
    Ensure every filename is *relative* to RAW_FOLDER, e.g.

        "raw_data/doc_3.txt" -> "doc_3.txt"
        "doc_3.txt"          -> "doc_3.txt"
    """
    prefix = RAW_FOLDER + os.sep
    return fname[len(prefix) :] if fname.startswith(prefix) else fname


def read_in_batches(files: List[str], batch_sz: int):
    """
    Yield batches of DocumentDTO-like dicts (from TextLoader.model_dump()).
    Accepts either absolute, relative-to-project, or bare filenames.
    """
    batch = []
    for fname in files:
        path = fname
        if not os.path.isabs(path) and not os.path.exists(path):
            path = os.path.join(RAW_FOLDER, fname)

        loader = TextLoader(path)
        for doc in loader.load():
            batch.append(doc.model_dump())
            if len(batch) >= batch_sz:
                yield batch
                batch = []
    if batch:
        yield batch


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    # ── 1. DOC-LEVEL SYNC ─────────────────────────────────────────────────────
    print("=== DOC-LEVEL SYNC ===")
    run_id = requests.post(f"{BASE_URL}/docs/start").json()["run_id"]

    txt_files = [f for f in os.listdir(RAW_FOLDER) if f.lower().endswith(".txt")]
    changed_set: Set[str] = set()

    for doc_batch in read_in_batches(txt_files, BATCH_SIZE):
        resp = requests.post(
            f"{BASE_URL}/docs/upload",
            params={"run_id": run_id},
            json=doc_batch,
        )
        resp.raise_for_status()
        data = resp.json()

        # normalise “raw_data/foo.txt” -> “foo.txt”
        changed_set.update(normalise(cid) for cid in data["changed_ids"])
        print("Batch result:", data)

    finish_resp = requests.post(f"{BASE_URL}/docs/finish", params={"run_id": run_id})
    finish_resp.raise_for_status()
    finish_json = finish_resp.json()
    deleted_ids = [normalise(cid) for cid in finish_json["deleted_ids"]]

    print(
        "Finish result:",
        finish_json,
        f"(local docs sent = {len(txt_files)})",
    )

    # ── Short-circuit if *nothing* changed ────────────────────────────────────
    if not changed_set and not deleted_ids:
        print("Everything identical – chunk stage skipped ✔")
        return

    # ── 2. CHUNK-LEVEL SYNC (add / update) ───────────────────────────────────
    if changed_set:
        print("\n=== CHUNK-LEVEL SYNC (add/update) ===")
        chunk_run_id = requests.post(f"{BASE_URL}/chunks/start").json()["chunk_run_id"]

        for doc_batch in read_in_batches(sorted(changed_set), BATCH_SIZE):
            resp = requests.post(
                f"{BASE_URL}/chunks/upload",
                params={"chunk_run_id": chunk_run_id},
                json=doc_batch,
            )
            resp.raise_for_status()
            print("Chunk batch result:", resp.json())

        # call /chunks/finish *without* cleanup so unchanged vectors stay
        requests.post(
            f"{BASE_URL}/chunks/finish",
            params={"chunk_run_id": chunk_run_id, "cleanup": "false"},
        ).raise_for_status()

    # ── 3. CHUNK-LEVEL DELETE (docs removed locally) ─────────────────────────
    if deleted_ids:
        print("\n=== CHUNK-LEVEL DELETE ===")
        del_resp = requests.post(
            f"{BASE_URL}/chunks/delete", json={"doc_ids": deleted_ids}
        )
        del_resp.raise_for_status()
        print("Deleted chunks result:", del_resp.json())


# ── entry-point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
