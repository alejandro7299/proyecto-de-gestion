"""
Excel Importer - Robusto para archivos sucios de flota
=======================================================
Estrategia:
1. Detectar automáticamente el header real buscando "Patente"
2. Ignorar todo lo que esté antes (resúmenes, títulos, celdas combinadas)
3. Limpiar y normalizar nombres de columnas
4. Asegurar consistencia con el schema de la BD
5. Insertar dinámicamente, creando columnas si faltan
"""

import pandas as pd
import numpy as np
import unicodedata
import re
import logging
from typing import Optional
from io import BytesIO

logger = logging.getLogger(__name__)

# ── Columnas canónicas del sistema ──────────────────────────────────────────
CANONICAL_COLUMNS = {
    "patente": "patente",
    "marca": "marca",
    "modelo": "modelo",
    "comprobante": "comprobante",
    "fecha": "fecha",
    "mes": "mes",
    "observacion": "observacion",
    "observación": "observacion",
    "detalle": "detalle",
    "accion": "accion",
    "acción": "accion",
    "subrubro": "subrubro",
    "insumo": "insumo",
    "nominsumo": "nominsumo",
    "nom_insumo": "nominsumo",
    "numinsumo": "nominsumo",
    "taller": "taller",
    "cantidad": "cantidad",
    "precio_unit": "precio_unit",
    "precio unit": "precio_unit",
    "preciounit": "precio_unit",
    "precio_unitario": "precio_unit",
    "costo": "costo",
    "centrocosto": "centrocosto",
    "centro_costo": "centrocosto",
    "operador": "operador",
    "n_ot": "n_ot",
    "not": "n_ot",
    "n°_ot": "n_ot",
    "numero_ot": "n_ot",
    "panol": "panol",
    "pañol": "panol",
    "inicio_ot": "inicio_ot",
    "tecnico": "tecnico",
    "técnico": "tecnico",
    "cumplida": "cumplida",
    "fin_ot": "fin_ot",
    "operacion": "operacion",
    "operación": "operacion",
    "fletero": "fletero",
    "empresa": "empresa",
    "rubro": "rubro",
}

NUMERIC_COLUMNS = {"cantidad", "precio_unit", "costo"}
DEFAULT_FILL = "SIN SELECCIONAR"


def normalize_col_name(name: str) -> str:
    """
    Normaliza un nombre de columna:
    - Quita tildes/diacríticos
    - Minúsculas
    - Reemplaza espacios y guiones por _
    - Elimina caracteres especiales (°, ñ→n, etc.)
    - Strip
    """
    if not isinstance(name, str):
        name = str(name)
    # Normalizar unicode → ASCII (quita tildes)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Minúsculas
    name = name.lower().strip()
    # ñ → n
    name = name.replace("ñ", "n")
    # Reemplazar espacios, /, \, - por _
    name = re.sub(r"[\s/\\-]+", "_", name)
    # Eliminar caracteres no alfanuméricos (excepto _)
    name = re.sub(r"[^\w]", "", name)
    # Eliminar _ múltiples
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def map_to_canonical(col: str) -> str:
    """Mapea un nombre normalizado al nombre canónico del sistema."""
    normalized = normalize_col_name(col)
    return CANONICAL_COLUMNS.get(normalized, normalized)


def find_header_row(df_raw: pd.DataFrame) -> Optional[int]:
    """
    Busca la fila donde aparece 'Patente' (case-insensitive) como encabezado.
    Retorna el índice de esa fila, o None si no se encuentra.
    """
    for i, row in df_raw.iterrows():
        for cell in row.values:
            if isinstance(cell, str) and "patente" in cell.lower().strip():
                logger.info(f"Header encontrado en fila {i}")
                return i
    return None


def load_excel_robust(file_obj) -> tuple[pd.DataFrame, dict]:
    """
    Carga un Excel sucio y retorna (DataFrame limpio, info de diagnóstico).
    
    Maneja:
    - Múltiples tablas en la misma hoja
    - Filas basura antes del header
    - Celdas combinadas
    - Columnas desplazadas
    - Nombres de columnas sucias
    """
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

    # ── 1. Leer sin asumir nada del header ─────────────────────────────────
    try:
        df_raw = pd.read_excel(
            file_obj,
            header=None,          # Sin header automático
            sheet_name=0,         # Primera hoja
            dtype=str,            # Todo como string para no perder datos
        )
    except Exception as e:
        raise ValueError(f"No se pudo leer el archivo Excel: {e}")

    report["total_rows_raw"] = len(df_raw)

    # ── 2. Detectar fila del header real ───────────────────────────────────
    header_row_idx = find_header_row(df_raw)
    if header_row_idx is None:
        # Fallback: intentar todas las hojas
        raise ValueError(
            "No se encontró la columna 'Patente' en el archivo. "
            "Verificá que el Excel contenga la tabla de datos correcta."
        )

    report["header_row"] = header_row_idx

    # ── 3. Extraer a partir del header ─────────────────────────────────────
    header_row = df_raw.iloc[header_row_idx].tolist()
    data_rows = df_raw.iloc[header_row_idx + 1:].copy()
    data_rows.columns = header_row
    data_rows = data_rows.reset_index(drop=True)

    # ── 4. Limpiar nombres de columnas ─────────────────────────────────────
    col_mapping = {}
    for col in data_rows.columns:
        if not isinstance(col, str) or col.strip() == "" or col.startswith("Unnamed"):
            continue
        canonical = map_to_canonical(col)
        col_mapping[col] = canonical

    report["columns_found"] = list(col_mapping.keys())
    report["columns_mapped"] = col_mapping

    # Renombrar columnas conocidas, eliminar las sin mapeo
    data_rows = data_rows.rename(columns=col_mapping)

    # Eliminar columnas duplicadas (celdas combinadas crean duplicados)
    data_rows = data_rows.loc[:, ~data_rows.columns.duplicated()]

    # Eliminar columnas completamente vacías o sin nombre válido
    valid_cols = [
        c for c in data_rows.columns
        if isinstance(c, str) and c.strip() and not c.startswith("none")
    ]
    data_rows = data_rows[valid_cols]

    # ── 5. Eliminar filas basura post-header ───────────────────────────────
    # Una fila es útil si tiene al menos patente O al menos 3 campos no vacíos
    def is_valid_row(row):
        vals = [v for v in row.values if isinstance(v, str) and v.strip() and v.strip().lower() not in ("nan", "none")]
        return len(vals) >= 3

    before = len(data_rows)
    data_rows = data_rows[data_rows.apply(is_valid_row, axis=1)]
    report["rows_skipped"] = before - len(data_rows)

    # ── 6. Normalizar valores ───────────────────────────────────────────────
    # Reemplazar NaN, "nan", "None", "" por DEFAULT_FILL en texto
    for col in data_rows.columns:
        if col in NUMERIC_COLUMNS:
            data_rows[col] = pd.to_numeric(data_rows[col], errors="coerce")
        else:
            data_rows[col] = data_rows[col].apply(
                lambda x: DEFAULT_FILL
                if (pd.isna(x) or str(x).strip().lower() in ("nan", "none", ""))
                else str(x).strip()
            )

    # Columnas numéricas con NaN → 0
    for col in NUMERIC_COLUMNS:
        if col in data_rows.columns:
            data_rows[col] = data_rows[col].fillna(0.0)

    # ── 7. Asegurar que todas las columnas canónicas existan ───────────────
    all_canonical = set(CANONICAL_COLUMNS.values())
    for col in all_canonical:
        if col not in data_rows.columns:
            data_rows[col] = DEFAULT_FILL if col not in NUMERIC_COLUMNS else 0.0

    # Identificar columnas desconocidas (extras del Excel)
    report["columns_unknown"] = [
        c for c in data_rows.columns if c not in all_canonical
    ]

    report["rows_imported"] = len(data_rows)

    logger.info(
        f"Import OK: {report['rows_imported']} filas, "
        f"header en fila {header_row_idx}, "
        f"{report['rows_skipped']} filas ignoradas"
    )

    return data_rows, report


def insert_dataframe(conn, df: pd.DataFrame, report: dict) -> int:
    """
    Inserta el DataFrame en la BD.
    Compatible con SQLite y PostgreSQL.
    """
    from db import add_column_if_missing, USE_SQLITE

    # Verificar columnas existentes
    cur = conn.cursor()
    if USE_SQLITE:
        cur.execute("PRAGMA table_info(flota)")
        existing_cols = {row[1].lower() for row in cur.fetchall()}
    else:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'flota'
        """)
        existing_cols = {row["column_name"].lower() for row in cur.fetchall()}
    cur.close()

    # Agregar columnas extra que no existan
    for col in df.columns:
        if col.lower() not in existing_cols and col not in ("id", "created_at"):
            col_type = "REAL" if col in NUMERIC_COLUMNS else "TEXT"
            add_column_if_missing(conn, "flota", col, col_type)
            report["warnings"].append(f"Columna nueva agregada a la BD: '{col}'")

    # Insertar en lotes
    cols = [c for c in df.columns if c not in ("id", "created_at")]
    ph = "?" if USE_SQLITE else "%s"
    placeholders = ", ".join([ph] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO flota ({col_names}) VALUES ({placeholders})"

    rows = [tuple(row[c] for c in cols) for _, row in df.iterrows()]

    cur = conn.cursor()
    cur.executemany(sql, rows)
    cur.close()
    conn.commit()

    return len(rows)
