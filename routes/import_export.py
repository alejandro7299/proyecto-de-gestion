from flask import Blueprint, request, flash, redirect, url_for, jsonify, send_file
from db import get_db, USE_SQLITE
from services.importer import load_excel_robust, insert_dataframe
import pandas as pd
from io import BytesIO
import traceback
import logging
from datetime import datetime
 
logger = logging.getLogger(__name__)
import_export_bp = Blueprint("import_export", __name__)
 
DB_LABEL = "SQLite local" if USE_SQLITE else "PostgreSQL"
 
 
def _fetchone_count(conn):
    """Obtiene COUNT(*) compatible con SQLite y PostgreSQL."""
    row = conn.execute("SELECT COUNT(*) FROM flota").fetchone()
    # SQLite retorna Row indexable, PostgreSQL retorna RealDictRow
    if isinstance(row, dict):
        return list(row.values())[0]
    return row[0]
 
 
def _fetch_chunks(conn, count, chunk=2000):
    """Lee la tabla en chunks para no reventar memoria."""
    chunks = []
    if USE_SQLITE:
        for offset in range(0, count, chunk):
            rows = conn.execute(
                "SELECT * FROM flota ORDER BY id ASC LIMIT ? OFFSET ?",
                (chunk, offset)
            ).fetchall()
            chunks.append(pd.DataFrame([dict(r) for r in rows]))
    else:
        # PostgreSQL usa %s como placeholder
        for offset in range(0, count, chunk):
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM flota ORDER BY id ASC LIMIT %s OFFSET %s",
                (chunk, offset)
            )
            rows = cur.fetchall()
            cur.close()
            chunks.append(pd.DataFrame(rows))
    return chunks
 
 
@import_export_bp.route("/import", methods=["POST"])
def import_excel():
    """Importación robusta de Excel. Devuelve JSON con resultado detallado."""
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No se recibió ningún archivo"}), 400
 
    file = request.files["file"]
    if not file.filename:
        return jsonify({"success": False, "error": "Nombre de archivo vacío"}), 400
 
    allowed = {".xlsx", ".xls", ".xlsm"}
    ext = "." + file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in allowed:
        return jsonify({
            "success": False,
            "error": f"Formato no soportado: '{ext}'. Usá .xlsx, .xls o .xlsm"
        }), 400
 
    try:
        file_bytes = BytesIO(file.read())
        df, report = load_excel_robust(file_bytes)
 
        if df.empty:
            return jsonify({
                "success": False,
                "error": "El archivo no contiene datos válidos después de la limpieza.",
                "report": report
            }), 422
 
        conn = get_db()
        try:
            rows_inserted = insert_dataframe(conn, df, report)
        finally:
            conn.close()
 
        logger.info(f"Import OK: {rows_inserted} filas insertadas en {DB_LABEL}")
 
        return jsonify({
            "success": True,
            "message": f"✅ {rows_inserted} registros importados correctamente",
            "report": {
                "header_encontrado_en_fila": report["header_row"],
                "filas_importadas": report["rows_imported"],
                "filas_ignoradas": report["rows_skipped"],
                "columnas_desconocidas": report["columns_unknown"],
                "advertencias": report["warnings"],
                "db": DB_LABEL,
            }
        })
 
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 422
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": f"Error inesperado: {str(e)}"
        }), 500
 
 
@import_export_bp.route("/export")
def export_excel():
    """Exporta TODOS los registros a Excel en chunks para soportar 14k+ filas."""
    conn = get_db()
    try:
        count = _fetchone_count(conn)
        print(f"[EXPORT] {DB_LABEL} | Registros: {count}")
 
        if count == 0:
            flash("La base de datos está vacía.", "warning")
            return redirect(url_for("main.index"))
 
        chunks = _fetch_chunks(conn, count)
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
 
    finally:
        conn.close()
 
    print(f"[EXPORT] DataFrame: {len(df)} filas x {len(df.columns)} columnas")
 
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Flota")
        ws = writer.sheets["Flota"]
        for col_cells in ws.columns:
            max_len = max(
                (len(str(c.value)) for c in col_cells if c.value is not None),
                default=10
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 40)
 
    output.seek(0)
    filename = f"flota_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
 
    response = send_file(
        output,
        download_name=filename,
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response.headers["X-Rows-Exported"] = str(len(df))
    response.headers["X-DB"] = DB_LABEL
    return response
 
 
@import_export_bp.route("/db-status")
def db_status():
    """Diagnóstico — muestra estado real de la BD. Visitá /db-status para verificar."""
    conn = get_db()
    try:
        count = _fetchone_count(conn)
 
        if USE_SQLITE:
            rows = conn.execute(
                "SELECT id, patente, fecha, costo FROM flota ORDER BY id DESC LIMIT 5"
            ).fetchall()
        else:
            cur = conn.cursor()
            cur.execute("SELECT id, patente, fecha, costo FROM flota ORDER BY id DESC LIMIT 5")
            rows = cur.fetchall()
            cur.close()
 
        return jsonify({
            "db": DB_LABEL,
            "total_rows": count,
            "last_5_rows": [dict(r) for r in rows],
        })
    finally:
        conn.close()
 
 