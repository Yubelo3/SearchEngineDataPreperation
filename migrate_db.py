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

    # try:
    #     with open(stopwords_file, "r", encoding="utf-8") as f:
    #         stopwords = set(line.strip() for line in f)
    # except FileNotFoundError:
    #     raise ValueError(f"Stopwords file not found: {stopwords_file}")

    # filtered_dict = {k: v for k, v in data.items() if k not in stopwords}

    reverse_dict = {v: k for k, v in data.items()}

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
        documents = [{"id": doc[0], "count": doc[1]} for doc in entry["doc"]]

        transformed.append((term_id, term_mapping[term_id], documents))
    return transformed


def transform_index_data_with_tfidf(index_data, tfidf_vectors, magnitudes):
    transformed = []
    for entry in index_data:
        term_id = entry[0]
        term = entry[1]
        documents = {}
        for doc in entry[2]:
            doc_id = doc["id"]
            tfidf = tfidf_vectors.get(doc_id, {}).get(term_id, 0.0)
            mag = magnitudes.get(doc_id, 0)
            tfidfM = tfidf / mag if mag != 0 else 0.0
            if tfidfM == 0:
                print("error cal")
            documents[doc_id] = tfidfM

        transformed.append((term_id, term, documents))
    return transformed


def extract_continuous_sequences(positions, terms):
    pairs = sorted(zip(positions, terms), key=lambda x: x[0])
    sequences = []
    current_seq = []
    prev_pos = None

    for pos, term in pairs:
        if prev_pos is None or pos == prev_pos + 1:
            current_seq.append(term)
        else:
            if len(current_seq) >= 1:
                sequences.append(current_seq)
            current_seq = [term]
        prev_pos = pos

    if current_seq:
        sequences.append(current_seq)
    return sequences


def transform_n_gram_data(forward_index_file_path, term_mapping, n):
    title_ngrams = defaultdict(lambda: defaultdict(int))
    body_ngrams = defaultdict(lambda: defaultdict(int))

    with open(forward_index_file_path, "r") as f:
        forward_index = json.load(f)

    for doc in forward_index:
        doc_id = doc["id"]

        title_terms = [term_mapping[tid] for tid in doc["title"]]
        title_sequences = extract_continuous_sequences(
            doc["title_word_pos"], title_terms
        )

        body_terms = [term_mapping[tid] for tid in doc["body"]]
        body_sequences = extract_continuous_sequences(doc["body_word_pos"], body_terms)

        def process_section(sequences, storage):
            for seq in sequences:
                seq_length = len(seq)
                for m in range(2, n + 1):
                    if seq_length < m:
                        continue
                    for i in range(seq_length - m + 1):
                        ngram = " ".join(seq[i : i + m])
                        storage[(ngram, m)][doc_id] += 1

        process_section(title_sequences, title_ngrams)
        process_section(body_sequences, body_ngrams)

    title_output = [
        (term, n_gram, dict(docs)) for (term, n_gram), docs in title_ngrams.items()
    ]

    body_output = [
        (term, n_gram, dict(docs)) for (term, n_gram), docs in body_ngrams.items()
    ]

    return title_output, body_output


def transform_n_gram_data_with_tfidf(
    n_gram_data, total_document_count, max_tf_dict, doc_mag_dict
):
    n_gram_tfidf = []

    for entry in n_gram_data:
        term, n, docs = entry
        df = len(docs)

        tfidf_docs = {}
        for doc_id, count in docs.items():
            max_tf = max_tf_dict.get(doc_id, 1)
            tf = count / max_tf

            idf = math.log((total_document_count + 1) / (df + 1))

            doc_magnitude = doc_mag_dict.get(doc_id, 1)
            tfidf = (tf * idf) / doc_magnitude

            tfidf_docs[doc_id] = tfidf

        n_gram_tfidf.append((term, n, tfidf_docs))

    return n_gram_tfidf


def transform_metadata_data(
    metadata_file, id_to_url_mapping, max_title_tf_dict, max_body_tf_dict
):
    with open(metadata_file) as f:
        metadata = json.load(f)

    transformed = []
    max_page_rank = 0
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
        if doc["pagerank"] > max_page_rank:
            max_page_rank = doc["pagerank"]
    return transformed, max_page_rank


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


def cal_mangitude(vector):
    sum = 0
    for v in vector.values():
        sum += v**2
    return round(math.sqrt(sum), 4)


def main():
    id_to_term = load_dictionary("page_data/dictionary.json")
    title_data = transform_index_data("page_data/title_inverted_index.json", id_to_term)
    body_data = transform_index_data("page_data/body_inverted_index.json", id_to_term)

    max_title_tf_dict = calculate_max_tf(title_data)
    max_body_tf_dict = calculate_max_tf(body_data)

    with open("page_data/metadata.json") as f:
        metadata = json.load(f)
    id_to_url = {doc["id"]: doc["url"] for doc in metadata}
    meta_data, max_page_rank = transform_metadata_data(
        "page_data/metadata.json", id_to_url, max_title_tf_dict, max_body_tf_dict
    )

    total_docs = len(meta_data)

    title_tfidf = calculate_tfidf_vectors(title_data, total_docs, max_title_tf_dict)

    body_tfidf = calculate_tfidf_vectors(body_data, total_docs, max_body_tf_dict)

    title_mags = {doc_id: cal_mangitude(vec) for doc_id, vec in title_tfidf.items()}
    body_mags = {doc_id: cal_mangitude(vec) for doc_id, vec in body_tfidf.items()}

    title_data_tfidf = transform_index_data_with_tfidf(
        title_data, title_tfidf, title_mags
    )
    body_data_tfidf = transform_index_data_with_tfidf(body_data, body_tfidf, body_mags)

    title_n_gram_data, body_n_gram_data = transform_n_gram_data(
        "page_data/forward_index.json", id_to_term, 4
    )
    title_n_gram_tfidf = transform_n_gram_data_with_tfidf(
        title_n_gram_data, total_docs, max_title_tf_dict, title_mags
    )
    body_n_gram_tfidf = transform_n_gram_data_with_tfidf(
        body_n_gram_data, total_docs, max_body_tf_dict, body_mags
    )

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("db connected\n")
    try:
        print("truncating all tables\n")
        cursor.execute(
            """TRUNCATE TABLE 
                title_inverted_index, 
                body_inverted_index, 
                document_meta
             CASCADE"""
        )

        print("start title_inverted_index migration\n")
        execute_batch(
            cursor,
            """INSERT INTO title_inverted_index (term, ngram, documents)
               VALUES (%s, %s, %s::jsonb)
               ON CONFLICT (term) DO UPDATE SET
                   documents = EXCLUDED.documents,
                   updated_at = CURRENT_TIMESTAMP""",
            [(term, 1, Json(docs)) for id, term, docs in title_data_tfidf],
            page_size=100,
        )
        print("finished title_inverted_index migration\n")

        print("start body_inverted_index migration\n")
        execute_batch(
            cursor,
            """INSERT INTO body_inverted_index (term, ngram, documents)
               VALUES (%s, %s, %s::jsonb)
               ON CONFLICT (term) DO UPDATE SET
                   documents = EXCLUDED.documents,
                   updated_at = CURRENT_TIMESTAMP""",
            [(term, 1, Json(docs)) for id, term, docs in body_data_tfidf],
            page_size=100,
        )
        print("finished body_inverted_index migration\n")

        print("start title_n_gram_inverted_index migration\n")
        execute_batch(
            cursor,
            """INSERT INTO title_inverted_index (term, ngram, documents)
               VALUES (%s, %s, %s::jsonb)
               ON CONFLICT (term) DO UPDATE SET
                   documents = EXCLUDED.documents,
                   updated_at = CURRENT_TIMESTAMP""",
            [(term, n, Json(docs)) for term, n, docs in title_n_gram_tfidf],
            page_size=100,
        )
        print("finished title_n_gram_inverted_index migration\n")

        print("start body_n_gram_inverted_index migration\n")
        execute_batch(
            cursor,
            """INSERT INTO body_inverted_index (term, ngram, documents)
               VALUES (%s, %s, %s::jsonb)
               ON CONFLICT (term) DO UPDATE SET
                   documents = EXCLUDED.documents,
                   updated_at = CURRENT_TIMESTAMP""",
            [(term, n, Json(docs)) for term, n, docs in body_n_gram_tfidf],
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
                    m[10] / max_page_rank,
                )
                for m in meta_data
            ],
            page_size=100,
        )
        print("finished document_meta migration\n")

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
