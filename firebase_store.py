import json
import os
from typing import List

import pandas as pd

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:  # pragma: no cover
    firebase_admin = None
    credentials = None
    firestore = None


_DB = None


def _load_service_account_dict():
    raw_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
    path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH", "").strip()

    if raw_json:
        return json.loads(raw_json)
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def is_enabled() -> bool:
    if firebase_admin is None:
        return False
    if os.getenv("USE_FIREBASE", "").strip().lower() not in ("1", "true", "yes", "on"):
        return False
    return _load_service_account_dict() is not None


def get_db():
    global _DB
    if _DB is not None:
        return _DB
    if not is_enabled():
        return None

    if not firebase_admin._apps:
        cred_dict = _load_service_account_dict()
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)

    _DB = firestore.client()
    return _DB


def load_collection_df(collection_name: str, columns: List[str]) -> pd.DataFrame:
    db = get_db()
    if db is None:
        return pd.DataFrame(columns=columns)

    docs = db.collection(collection_name).stream()
    rows = []
    for d in docs:
        row = d.to_dict() or {}
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df[columns]


def save_collection_df(collection_name: str, df: pd.DataFrame) -> None:
    db = get_db()
    if db is None:
        return

    col = db.collection(collection_name)
    existing = list(col.stream())

    batch = db.batch()
    op_count = 0
    for doc in existing:
        batch.delete(doc.reference)
        op_count += 1
        if op_count >= 450:
            batch.commit()
            batch = db.batch()
            op_count = 0

    records = df.to_dict(orient="records")
    for i, row in enumerate(records):
        batch.set(col.document(f"row_{i:08d}"), row)
        op_count += 1
        if op_count >= 450:
            batch.commit()
            batch = db.batch()
            op_count = 0

    if op_count > 0:
        batch.commit()
