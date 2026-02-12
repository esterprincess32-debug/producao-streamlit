import json
import os
import re
import time
import uuid
from typing import List, Optional

import pandas as pd

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:  # pragma: no cover
    firebase_admin = None
    credentials = None
    firestore = None


_DB = None
_CACHE = {}


def _cache_ttl_seconds() -> int:
    raw = os.getenv("FIREBASE_CACHE_TTL_SECONDS", "").strip()
    try:
        v = int(raw)
        return max(0, v)
    except Exception:
        return 3


def _collection_cache_key(collection_name: str, columns: List[str], order_by: Optional[str], descending: bool, limit: Optional[int]):
    return (
        "load_collection_df",
        collection_name,
        tuple(columns),
        str(order_by or ""),
        bool(descending),
        int(limit) if limit else 0,
    )


def _cache_get(key):
    ttl = _cache_ttl_seconds()
    if ttl <= 0:
        return None
    item = _CACHE.get(key)
    if not item:
        return None
    ts, value = item
    if (time.time() - ts) > ttl:
        _CACHE.pop(key, None)
        return None
    return value


def _cache_set(key, value):
    ttl = _cache_ttl_seconds()
    if ttl <= 0:
        return
    _CACHE[key] = (time.time(), value)


def _invalidate_collection_cache(collection_name: str):
    keys = [k for k in _CACHE.keys() if isinstance(k, tuple) and len(k) >= 2 and k[1] == collection_name]
    for k in keys:
        _CACHE.pop(k, None)


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


def load_collection_df(
    collection_name: str,
    columns: List[str],
    order_by: Optional[str] = None,
    descending: bool = False,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    cache_key = _collection_cache_key(collection_name, columns, order_by, descending, limit)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached.copy(deep=True)

    db = get_db()
    if db is None:
        return pd.DataFrame(columns=columns)

    query = db.collection(collection_name)
    if order_by:
        direction = firestore.Query.DESCENDING if descending else firestore.Query.ASCENDING
        query = query.order_by(order_by, direction=direction)
    if limit and int(limit) > 0:
        query = query.limit(int(limit))

    docs = query.stream()
    rows = []
    for d in docs:
        row = d.to_dict() or {}
        rows.append(row)

    if not rows:
        out = pd.DataFrame(columns=columns)
        _cache_set(cache_key, out)
        return out

    df = pd.DataFrame(rows)
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    out = df[columns]
    _cache_set(cache_key, out)
    return out.copy(deep=True)


def _safe_doc_id(value) -> str:
    raw = str(value).strip()
    if not raw:
        raw = str(uuid.uuid4())
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", raw)
    return safe[:1500]


def save_collection_df(collection_name: str, df: pd.DataFrame, key_field: str = None) -> None:
    db = get_db()
    if db is None:
        return

    col = db.collection(collection_name)
    if key_field and key_field in df.columns:
        existing = {d.id: d.reference for d in col.stream()}
        desired_ids = set()

        batch = db.batch()
        op_count = 0
        for _, row in df.iterrows():
            doc_id = _safe_doc_id(row.get(key_field, ""))
            desired_ids.add(doc_id)
            payload = {}
            for k, v in row.to_dict().items():
                payload[k] = v.item() if hasattr(v, "item") else v
            batch.set(col.document(doc_id), payload)
            op_count += 1
            if op_count >= 450:
                batch.commit()
                batch = db.batch()
                op_count = 0

        for doc_id, doc_ref in existing.items():
            if doc_id not in desired_ids:
                batch.delete(doc_ref)
                op_count += 1
                if op_count >= 450:
                    batch.commit()
                    batch = db.batch()
                    op_count = 0

        if op_count > 0:
            batch.commit()
        _invalidate_collection_cache(collection_name)
        return

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
        payload = {}
        for k, v in row.items():
            payload[k] = v.item() if hasattr(v, "item") else v
        batch.set(col.document(f"row_{i:08d}"), payload)
        op_count += 1
        if op_count >= 450:
            batch.commit()
            batch = db.batch()
            op_count = 0

    if op_count > 0:
        batch.commit()
    _invalidate_collection_cache(collection_name)


def append_document(collection_name: str, data: dict) -> None:
    db = get_db()
    if db is None:
        return
    payload = {}
    for k, v in data.items():
        payload[k] = v.item() if hasattr(v, "item") else v
    db.collection(collection_name).document().set(payload)
    _invalidate_collection_cache(collection_name)


def get_latest_field_value(collection_name: str, field_name: str):
    db = get_db()
    if db is None:
        return None

    docs = db.collection(collection_name).order_by(field_name, direction=firestore.Query.DESCENDING).limit(1).stream()
    for doc in docs:
        data = doc.to_dict() or {}
        return data.get(field_name)
    return None
