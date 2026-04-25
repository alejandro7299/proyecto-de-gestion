from flask import Blueprint, request, flash, redirect, url_for, jsonify, send_file
from db import get_db, DB_PATH
from services.importer import load_excel_robust, insert_dataframe
import pandas as pd
from io import BytesIO
import traceback
import logging
import os
 
logger = logging.getLogger(__name__)
import_export_bp = Blueprint("import_export", __name__)
 
 
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
 
        logger.info(f"Import OK: {rows_inserted} filas insertadas en {DB_PATH}")
 
        return jsonify({
            "success": True,
            "message": f"✅ {rows_inserted} registros importados correctamente",
            "report": {
                "header_encontrado_en_fila": report["header_row"],
                "filas_importadas": report["rows_imported"],
                "filas_ignoradas": report["rows_skipped"],
                "columnas_mapeadas": report["columns_mapped"],
                "columnas_desconocidas": report["columns_unknown"],
                "advertencias": report["warnings"],
                "db_path": DB_PATH,  # visible en respuesta para debug
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
    """
    Exporta TODOS los registros a Excel.
    
    BUG ORIGINAL: Si get_db() resolvía a un archivo diferente al que tiene
    los datos, devolvía 0-2 filas (las del schema vacío o una importación previa).
    Ahora logueamos el path y el count antes de exportar para diagnóstico.
    """
    conn = get_db()
    try:
        # Diagnóstico: loguear path y cantidad ANTES de leer
        count = conn.execute("SELECT COUNT(*) FROM flota").fetchone()[0]
        logger.info(f"Export: {count} registros en {DB_PATH}")
        print(f"[EXPORT] DB: {DB_PATH} | Registros: {count}")
 
        if count == 0:
            flash(f"La base de datos está vacía. (DB: {DB_PATH})", "warning")
            return redirect(url_for("main.index"))
 
        # Leer en chunks para no reventar memoria con 14k+ filas
        CHUNK = 5000
        chunks = []
        for offset in range(0, count, CHUNK):
            rows = conn.execute(
                "SELECT * FROM flota ORDER BY id ASC LIMIT ? OFFSET ?",
                (CHUNK, offset)
            ).fetchall()
            chunks.append(pd.DataFrame([dict(r) for r in rows]))
 
        df = pd.concat(chunks, ignore_index=True)
 
    finally:
        conn.close()
 
    logger.info(f"Export: DataFrame construido con {len(df)} filas, {len(df.columns)} columnas")
    print(f"[EXPORT] DataFrame: {len(df)} filas")
 
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Flota")
 
        # Auto-ajustar anchos (rápido)
        ws = writer.sheets["Flota"]
        for col_cells in ws.columns:
            max_len = max(
                (len(str(c.value)) for c in col_cells if c.value is not None),
                default=10
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 40)
 
    output.seek(0)
 
    from datetime import datetime
    filename = f"flota_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
 
    response = send_file(
        output,
        download_name=filename,
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    # Header de debug — visible en DevTools
    response.headers["X-Rows-Exported"] = str(len(df))
    response.headers["X-DB-Path"] = DB_PATH
    return response
 
 
@import_export_bp.route("/db-status")
def db_status():
    """
    Endpoint de diagnóstico — muestra estado real de la BD.
    Acceder a /db-status para ver si los datos están donde deben estar.
    """
    conn = get_db()
    try:
        count = conn.execute("SELECT COUNT(*) FROM flota").fetchone()[0]
        sample = conn.execute(
            "SELECT id, patente, fecha, costo FROM flota ORDER BY id DESC LIMIT 5"
        ).fetchall()
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
 
        return jsonify({
            "db_path": DB_PATH,
            "db_exists": os.path.exists(DB_PATH),
            "db_size_mb": round(db_size / 1024 / 1024, 2),
            "total_rows": count,
            "last_5_rows": [dict(r) for r in sample],
        })
    finally:
        conn.close()
 
 