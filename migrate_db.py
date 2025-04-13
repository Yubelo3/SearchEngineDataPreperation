from collections import defaultdict
import json
import math
import psycopg2
from psycopg2.extras import execute_batch, Json

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "se_db",
    "user": "postgres",
    "password": "test123",
}


def load_dictionary(file_path, stopwords_file="stopwords.txt"):
    with open(file_path) as f:
        data = json.load(f)

    try:
        with open(stopwords_file, "r", encoding="utf-8") as f:
            stopwords = set(line.strip() for line in f)
    except FileNotFoundError:
        raise ValueError(f"Stopwords file not found: {stopwords_file}")

    filtered_dict = {k: v for k, v in data.items() if k not in stopwords}

    reverse_dict = {v: k for k, v in filtered_dict.items()}

    return reverse_dict


def transform_index_data(index_file, term_mapping):
    with open(index_file) as f:
        index_data = json.load(f)

    transformed = []
    for entry in index_data:
        term_id = entry["id"]
        if term_id not in term_mapping:
            continue
        if len(entry["doc"]) <= 0:
            continue
        documents = [
            {"id": doc[0], "count": doc[1], "pos": doc[2]} for doc in entry["doc"]
        ]

        transformed.append((term_id, term_mapping[term_id], documents))
    return transformed


def transform_metadata_data(
    metadata_file, id_to_url_mapping, max_title_tf_dict, max_body_tf_dict
):
    with open(metadata_file) as f:
        metadata = json.load(f)

    transformed = []
    for doc in metadata:
        parent_links = [
            id_to_url_mapping[parent_id]
            for parent_id in doc["parents_id"]
            if parent_id in id_to_url_mapping
        ]

        max_title_tf = max_title_tf_dict.get(doc["id"], 0)
        max_body_tf = max_body_tf_dict.get(doc["id"], 0)

        transformed.append(
            (
                doc["id"],
                doc["title"],
                doc["url"],
                doc["last_modified"],
                doc["size"],
                doc["freq_words"],
                parent_links,
                doc["links"],
                max_title_tf,
                max_body_tf,
                doc["pagerank"],
            )
        )
    return transformed


def calculate_max_tf(index_data):
    max_tf_dict = {}
    for entry in index_data:
        for doc in entry[2]:
            doc_id = doc["id"]
            if doc["count"] > max_tf_dict.get(doc_id, 0):
                max_tf_dict[doc_id] = doc["count"]
    return max_tf_dict


def calculate_tfidf_vectors(index_data, total_docs, max_tf_dict):
    term_doc_freq = {}
    for entry in index_data:
        term_id = entry[0]
        doc_count = len(entry[2])
        term_doc_freq[term_id] = doc_count

    idf_cache = {
        term_id: math.log((total_docs + 1) / (doc_freq + 1))
        for term_id, doc_freq in term_doc_freq.items()
    }

    doc_vectors = defaultdict(dict)
    for entry in index_data:
        term_id, term, docs = entry
        for doc in docs:
            doc_id = doc["id"]
            tf = doc["count"] / max(1, max_tf_dict.get(doc_id, 0))
            tfidf = tf * idf_cache[term_id]
            doc_vectors[doc_id][term_id] = round(tfidf, 4)

    return doc_vectors


def main():
    id_to_term = load_dictionary("page_data/dictionary.json")
    title_data = transform_index_data("page_data/title_inverted_index.json", id_to_term)
    body_data = transform_index_data("page_data/body_inverted_index.json", id_to_term)

    max_title_tf_dict = calculate_max_tf(title_data)
    max_body_tf_dict = calculate_max_tf(body_data)

    with open("page_data/metadata.json") as f:
        metadata = json.load(f)
    id_to_url = {doc["id"]: doc["url"] for doc in metadata}
    meta_data = transform_metadata_data(
        "page_data/metadata.json", id_to_url, max_title_tf_dict, max_body_tf_dict
    )

    total_docs = len(meta_data)

    title_tfidf = calculate_tfidf_vectors(title_data, total_docs, max_title_tf_dict)

    body_tfidf = calculate_tfidf_vectors(body_data, total_docs, max_body_tf_dict)

    tfidf_records = []
    for doc_id in title_tfidf.keys() | body_tfidf.keys():
        record = (
            doc_id,
            Json(title_tfidf.get(doc_id, {})),
            Json(body_tfidf.get(doc_id, {})),
        )
        tfidf_records.append(record)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("db connected\n")
    try:
        print("truncating all tables\n")
        cursor.execute(
            """TRUNCATE TABLE 
                title_inverted_index, 
                body_inverted_index, 
                document_meta, 
                document_tfidf 
             CASCADE"""
        )

        print("start title_inverted_index migration\n")
        execute_batch(
            cursor,
            """INSERT INTO title_inverted_index (id, term, documents)
               VALUES (%s, %s, %s::jsonb)
               ON CONFLICT (term) DO UPDATE SET
                   documents = EXCLUDED.documents,
                   updated_at = CURRENT_TIMESTAMP""",
            [(id, term, Json(docs)) for id, term, docs in title_data],
            page_size=100,
        )
        print("finished title_inverted_index migration\n")

        print("start body_inverted_index migration\n")
        execute_batch(
            cursor,
            """INSERT INTO body_inverted_index (id, term, documents)
               VALUES (%s, %s, %s::jsonb)
               ON CONFLICT (term) DO UPDATE SET
                   documents = EXCLUDED.documents,
                   updated_at = CURRENT_TIMESTAMP""",
            [(id, term, Json(docs)) for id, term, docs in body_data],
            page_size=100,
        )
        print("finished body_inverted_index migration\n")

        print("start document_meta migration\n")
        execute_batch(
            cursor,
            """INSERT INTO document_meta (
            id, title, url, last_modified, size, 
            freq_words, parent_links, child_links,
            max_title_tf, max_body_tf, page_rank
        ) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s,%s)
        ON CONFLICT (url) DO UPDATE SET
            title = EXCLUDED.title,
            last_modified = EXCLUDED.last_modified,
            size = EXCLUDED.size,
            freq_words = EXCLUDED.freq_words,
            parent_links = EXCLUDED.parent_links,
            child_links = EXCLUDED.child_links,
            max_title_tf = EXCLUDED.max_title_tf,
            max_body_tf = EXCLUDED.max_body_tf,
            page_rank = EXCLUDED.page_rank,
            updated_at = CURRENT_TIMESTAMP""",
            [
                (
                    m[0],
                    m[1],
                    m[2],
                    m[3],
                    m[4],
                    Json(m[5]),
                    Json(m[6]),
                    Json(m[7]),
                    m[8],
                    m[9],
                    m[10],
                )
                for m in meta_data
            ],
            page_size=100,
        )
        print("finished document_meta migration\n")

        print("start document_tfidf migration\n")
        execute_batch(
            cursor,
            """INSERT INTO document_tfidf (
                id, title_tfidf_vec, body_tfidf_vec
            ) VALUES (%s, %s::jsonb, %s::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                title_tfidf_vec = EXCLUDED.title_tfidf_vec,
                body_tfidf_vec = EXCLUDED.body_tfidf_vec,
                updated_at = CURRENT_TIMESTAMP""",
            tfidf_records,
            page_size=100,
        )
        print("finished document_tfidf migration\n")

        conn.commit()

        print("finished db migration\n")
    except Exception as e:
        conn.rollback()
        print(f"Error: {str(e)}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
