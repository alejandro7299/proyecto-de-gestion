"""
Excel Importer - Insert-on-the-fly
===================================
Lee el Excel fila por fila e inserta directamente en la BD
sin acumular datos en memoria. RAM constante sin importar
el tamaño del archivo.
"""
import unicodedata
import re
import logging
from openpyxl import load_workbook
 
logger = logging.getLogger(__name__)
 
CANONICAL_COLUMNS = {
    "patente": "patente", "marca": "marca", "modelo": "modelo",
    "comprobante": "comprobante", "fecha": "fecha", "mes": "mes",
    "observacion": "observacion", "detalle": "detalle",
    "accion": "accion", "subrubro": "subrubro", "insumo": "insumo",
    "nominsumo": "nominsumo", "nom_insumo": "nominsumo",
    "taller": "taller", "cantidad": "cantidad",
    "precio_unit": "precio_unit", "precio unit": "precio_unit",
    "preciounit": "precio_unit", "precio_unitario": "precio_unit",
    "costo": "costo", "centrocosto": "centrocosto",
    "centro_costo": "centrocosto", "operador": "operador",
    "n_ot": "n_ot", "panol": "panol",
    "inicio_ot": "inicio_ot", "tecnico": "tecnico",
    "cumplida": "cumplida", "fin_ot": "fin_ot",
    "operacion": "operacion", "fletero": "fletero",
    "empresa": "empresa", "rubro": "rubro",
}
 
NUMERIC_COLUMNS = {"cantidad", "precio_unit", "costo"}
DEFAULT_FILL = "SIN SELECCIONAR"
BATCH_SIZE = 200
 
 
def normalize_col(name):
    if not isinstance(name, str):
        name = str(name)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip().replace("n~", "n")
    name = re.sub(r"[\s/\\-]+", "_", name)
    name = re.sub(r"[^\w]", "", name)
    return re.sub(r"_+", "_", name).strip("_")
 
 
def to_canonical(col):
    return CANONICAL_COLUMNS.get(normalize_col(col), normalize_col(col))
 
 
def clean_val(val, col):
    if val is None:
        return 0.0 if col in NUMERIC_COLUMNS else DEFAULT_FILL
    s = str(val).strip()
    if s.lower() in ("none", "nan", ""):
        return 0.0 if col in NUMERIC_COLUMNS else DEFAULT_FILL
    if col in NUMERIC_COLUMNS:
        try:
            return float(s.replace(",", "."))
        except (ValueError, AttributeError):
            return 0.0
    return s
 
 
def import_excel_to_db(file_obj, conn) -> dict:
    """
    Lee el Excel e inserta directo en la BD fila por fila.
    Nunca acumula más de BATCH_SIZE filas en RAM.
    """
    from db import USE_SQLITE
 
    report = {
        "header_row": None,
        "rows_imported": 0,
        "rows_skipped": 0,
        "warnings": [],
    }
 
    wb = load_workbook(filename=file_obj, read_only=True, data_only=True)
    ws = wb.active
 
    header_idx = None
    canonical_cols = []
    all_cols = list(CANONICAL_COLUMNS.values())
    ph = "?" if USE_SQLITE else "%s"
 
    # Preparar SQL con todas las columnas canónicas
    placeholders = ", ".join([ph] * len(all_cols))
    sql = f"INSERT INTO flota ({', '.join(all_cols)}) VALUES ({placeholders})"
 
    batch = []
    cur = conn.cursor()
    row_num = 0
 
    for row in ws.iter_rows(values_only=True):
        row_num += 1
 
        # Buscar header
        if header_idx is None:
            for cell in row:
                if isinstance(cell, str) and "patente" in cell.lower().strip():
                    header_idx = row_num
                    report["header_row"] = row_num
                    canonical_cols = [
                        to_canonical(str(c)) if c is not None else None
                        for c in row
                    ]
                    break
            continue
 
        # Filtrar filas vacías
        vals = [v for v in row if v is not None and str(v).strip() not in ("", "None", "nan")]
        if len(vals) < 3:
            report["rows_skipped"] += 1
            continue
 
        # Construir dict de la fila
        row_dict = {}
        for i, val in enumerate(row):
            if i >= len(canonical_cols):
                break
            col = canonical_cols[i]
            if col:
                row_dict[col] = clean_val(val, col)
 
        # Construir tupla con todas las columnas canónicas en orden
        row_tuple = tuple(
            row_dict.get(c, 0.0 if c in NUMERIC_COLUMNS else DEFAULT_FILL)
            for c in all_cols
        )
        batch.append(row_tuple)
 
        # Insertar batch cuando llega al límite
        if len(batch) >= BATCH_SIZE:
            cur.executemany(sql, batch)
            conn.commit()
            report["rows_imported"] += len(batch)
            batch = []
 
    # Insertar el último batch
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        report["rows_imported"] += len(batch)
 
    cur.close()
    wb.close()
 
    if header_idx is None:
        raise ValueError(
            "No se encontro la columna 'Patente' en el archivo. "
            "Verifica que el Excel contenga la tabla de datos correcta."
        )
 
    return report
 
 
# Compatibilidad con el código existente en import_export.py
def load_excel_robust(file_obj):
    """Wrapper — retorna (None, report) para mantener interfaz."""
    return None, {"_file_obj": file_obj, "header_row": None,
                  "rows_imported": 0, "rows_skipped": 0,
                  "columns_mapped": {}, "columns_unknown": [], "warnings": []}
 
 
def insert_dataframe(conn, rows_data, report: dict) -> int:
    """Wrapper — si rows_data es None usamos el file_obj guardado."""
    if rows_data is None:
        raise RuntimeError("Usar import_excel_to_db() directamente")
    return 0
 