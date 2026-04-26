"""
Excel Importer - Robusto para archivos sucios de flota
"""
import pandas as pd
import unicodedata
import re
import logging
from typing import Optional
from io import BytesIO
 
logger = logging.getLogger(__name__)
 
CANONICAL_COLUMNS = {
    "patente": "patente", "marca": "marca", "modelo": "modelo",
    "comprobante": "comprobante", "fecha": "fecha", "mes": "mes",
    "observacion": "observacion", "observación": "observacion",
    "detalle": "detalle", "accion": "accion", "acción": "accion",
    "subrubro": "subrubro", "insumo": "insumo",
    "nominsumo": "nominsumo", "nom_insumo": "nominsumo",
    "taller": "taller", "cantidad": "cantidad",
    "precio_unit": "precio_unit", "precio unit": "precio_unit",
    "preciounit": "precio_unit", "precio_unitario": "precio_unit",
    "costo": "costo", "centrocosto": "centrocosto",
    "centro_costo": "centrocosto", "operador": "operador",
    "n_ot": "n_ot", "not": "n_ot", "n°_ot": "n_ot",
    "panol": "panol", "pañol": "panol",
    "inicio_ot": "inicio_ot", "tecnico": "tecnico",
    "técnico": "tecnico", "cumplida": "cumplida",
    "fin_ot": "fin_ot", "operacion": "operacion",
    "operación": "operacion", "fletero": "fletero",
    "empresa": "empresa", "rubro": "rubro",
}
 
NUMERIC_COLUMNS = {"cantidad", "precio_unit", "costo"}
DEFAULT_FILL = "SIN SELECCIONAR"
 
 
def normalize_col_name(name: str) -> str:
    if not isinstance(name, str):
        name = str(name)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    name = name.replace("ñ", "n")
    name = re.sub(r"[\s/\\-]+", "_", name)
    name = re.sub(r"[^\w]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name
 
 
def map_to_canonical(col: str) -> str:
    normalized = normalize_col_name(col)
    return CANONICAL_COLUMNS.get(normalized, normalized)
 
 
def find_header_row(df_raw: pd.DataFrame) -> Optional[int]:
    for i, row in df_raw.iterrows():
        for cell in row.values:
            if isinstance(cell, str) and "patente" in cell.lower().strip():
                logger.info(f"Header encontrado en fila {i}")
                return i
    return None
 
 
def load_excel_robust(file_obj) -> tuple:
    report = {
        "header_row": None,
        "total_rows_raw": 0,
        "rows_imported": 0,
        "rows_skipped": 0,
        "columns_found": [],
        "columns_mapped": {},
        "columns_unknown": [],
        "warnings": [],
    }
 
    try:
        df_raw = pd.read_excel(file_obj, header=None, sheet_name=0, dtype=str)
    except Exception as e:
        raise ValueError(f"No se pudo leer el archivo Excel: {e}")
 
    report["total_rows_raw"] = len(df_raw)
 
    header_row_idx = find_header_row(df_raw)
    if header_row_idx is None:
        raise ValueError(
            "No se encontró la columna 'Patente' en el archivo. "
            "Verificá que el Excel contenga la tabla de datos correcta."
        )
 
    report["header_row"] = header_row_idx
 
    header_row = df_raw.iloc[header_row_idx].tolist()
    data_rows = df_raw.iloc[header_row_idx + 1:].copy()
    data_rows.columns = header_row
    data_rows = data_rows.reset_index(drop=True)
 
    col_mapping = {}
    for col in data_rows.columns:
        if not isinstance(col, str) or col.strip() == "" or col.startswith("Unnamed"):
            continue
        canonical = map_to_canonical(col)
        col_mapping[col] = canonical
 
    report["columns_found"] = list(col_mapping.keys())
    report["columns_mapped"] = col_mapping
 
    data_rows = data_rows.rename(columns=col_mapping)
    data_rows = data_rows.loc[:, ~data_rows.columns.duplicated()]
 
    valid_cols = [
        c for c in data_rows.columns
        if isinstance(c, str) and c.strip() and not c.startswith("none")
    ]
    data_rows = data_rows[valid_cols]
 
    def is_valid_row(row):
        vals = [v for v in row.values if isinstance(v, str) and v.strip() and v.strip().lower() not in ("nan", "none")]
        return len(vals) >= 3
 
    before = len(data_rows)
    data_rows = data_rows[data_rows.apply(is_valid_row, axis=1)]
    report["rows_skipped"] = before - len(data_rows)
 
    for col in data_rows.columns:
        if col in NUMERIC_COLUMNS:
            data_rows[col] = pd.to_numeric(data_rows[col], errors="coerce")
        else:
            data_rows[col] = data_rows[col].apply(
                lambda x: DEFAULT_FILL
                if (pd.isna(x) or str(x).strip().lower() in ("nan", "none", ""))
                else str(x).strip()
            )
 
    for col in NUMERIC_COLUMNS:
        if col in data_rows.columns:
            data_rows[col] = data_rows[col].fillna(0.0)
 
    all_canonical = set(CANONICAL_COLUMNS.values())
    for col in all_canonical:
        if col not in data_rows.columns:
            data_rows[col] = DEFAULT_FILL if col not in NUMERIC_COLUMNS else 0.0
 
    report["columns_unknown"] = [c for c in data_rows.columns if c not in all_canonical]
    report["rows_imported"] = len(data_rows)
 
    return data_rows, report
 
 
def insert_dataframe(conn, df: pd.DataFrame, report: dict) -> int:
    from db import add_column_if_missing
 
    cursor = conn.execute("PRAGMA table_info(flota)")
    existing_cols = {row[1].lower() for row in cursor.fetchall()}
 
    for col in df.columns:
        if col.lower() not in existing_cols and col not in ("id", "created_at"):
            col_type = "REAL" if col in NUMERIC_COLUMNS else "TEXT"
            add_column_if_missing(conn, "flota", col, col_type)
            report["warnings"].append(f"Columna nueva agregada a la BD: '{col}'")
 
    cols = [c for c in df.columns if c not in ("id", "created_at")]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO flota ({col_names}) VALUES ({placeholders})"
 
    rows = [tuple(row[c] for c in cols) for _, row in df.iterrows()]
    conn.executemany(sql, rows)
    conn.commit()
 
    return len(rows)
 