import os
import requests
from langchain_community.document_loaders import TextLoader


def main():
    base_url = "http://127.0.0.1:8000"
    folder = "raw_data"
    batch_size = 50

    def read_in_batches(files, batch_sz):
        batch = []
        for fname in files:
            path = os.path.join(folder, fname)
            loader = TextLoader(path)
            docs = loader.load()
            for d in docs:
                batch.append(d.model_dump())
                if len(batch) >= batch_sz:
                    yield batch
                    batch = []
        if batch:
            yield batch

    print("=== DOC-LEVEL SYNC ===")
    # Update the URL to match the new endpoint
    start_resp = requests.post(f"{base_url}/docs/start")
    run_id = start_resp.json()["run_id"]

    txt_files = [f for f in os.listdir(folder) if f.lower().endswith(".txt")]
    total_docs = 0
    for doc_batch in read_in_batches(txt_files, batch_size):
        upload_resp = requests.post(
            f"{base_url}/docs/upload", params={"run_id": run_id}, json=doc_batch
        )
        total_docs += len(doc_batch)
        print("Batch result:", upload_resp.json())
    finish_resp = requests.post(f"{base_url}/docs/finish", params={"run_id": run_id})
    print("Finish result:", finish_resp.json(), f"(total docs sent={total_docs})")

    print("=== CHUNK-LEVEL SYNC ===")
    # Update the URL to match the new endpoint
    start_chunk_resp = requests.post(f"{base_url}/chunks/start")
    chunk_run_id = start_chunk_resp.json()["chunk_run_id"]

    total_chunk_docs = 0
    for doc_batch in read_in_batches(txt_files, batch_size):
        upload_chunk_resp = requests.post(
            f"{base_url}/chunks/upload",
            params={"chunk_run_id": chunk_run_id},
            json=doc_batch,
        )
        total_chunk_docs += len(doc_batch)
        print("Chunk batch result:", upload_chunk_resp.json())
    finish_chunk_resp = requests.post(
        f"{base_url}/chunks/finish", params={"chunk_run_id": chunk_run_id}
    )
    print(
        "Chunk finish result:",
        finish_chunk_resp.json(),
        f"(total chunk docs sent={total_chunk_docs})",
    )


if __name__ == "__main__":
    main()
