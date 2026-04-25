from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from db import get_db
from services.ai_engine import classify_alert

main_bp = Blueprint("main", __name__)

PAGE_SIZE = 200  # Rows shown in table — intentionally limited for browser perf


@main_bp.route("/")
def index():
    conn = get_db()
    try:
        # ── Filtros ──────────────────────────────────────────────────────────
        patente = request.args.get("patente", "").strip()
        taller  = request.args.get("taller",  "").strip()
        alerta  = request.args.get("alerta",  "").strip()
        page    = max(1, int(request.args.get("page", 1)))

        # ── KPIs reales sobre TODA la tabla (sin LIMIT) ─────────────────────
        # Bug original: los KPIs se calculaban sobre los 500 registros del LIMIT.
        kpi_row = conn.execute("""
            SELECT
                COUNT(*)                          AS total,
                COALESCE(SUM(costo), 0)           AS costo_total,
                COUNT(DISTINCT patente)           AS vehiculos,
                COUNT(DISTINCT taller)            AS talleres
            FROM flota
            WHERE patente != 'SIN SELECCIONAR' OR patente IS NULL
        """).fetchone()

        kpis = {
            "total":       kpi_row["total"],
            "costo_total": kpi_row["costo_total"],
            "vehiculos":   kpi_row["vehiculos"],
            "talleres":    kpi_row["talleres"],
        }

        # ── Query paginada con filtros ────────────────────────────────────────
        where_parts = ["1=1"]
        params = []

        if patente:
            where_parts.append("patente LIKE ?")
            params.append(f"%{patente}%")
        if taller:
            where_parts.append("taller LIKE ?")
            params.append(f"%{taller}%")

        where_clause = " AND ".join(where_parts)
        offset = (page - 1) * PAGE_SIZE

        # Count total filtered rows (for pagination)
        count_row = conn.execute(
            f"SELECT COUNT(*) AS n FROM flota WHERE {where_clause}", params
        ).fetchone()
        total_filtered = count_row["n"]
        total_pages = max(1, (total_filtered + PAGE_SIZE - 1) // PAGE_SIZE)

        rows = conn.execute(
            f"SELECT * FROM flota WHERE {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [PAGE_SIZE, offset]
        ).fetchall()
        data = [dict(r) for r in rows]

        # ── Clasificación IA (solo sobre los registros visibles) ─────────────
        for row in data:
            row["alerta_ia"] = classify_alert(row)

        # Filtro por alerta IA — nota: aplica solo sobre la página actual.
        # Para filtrar TODA la DB por IA, usar el endpoint /api/analysis.
        if alerta:
            data = [r for r in data if r["alerta_ia"] == alerta]

        # Contar críticos reales en página actual
        criticos = sum(1 for r in data if r["alerta_ia"] == "CRÍTICO")
        kpis["criticos"] = criticos  # aproximado — exacto en dashboard IA

        # ── Opciones de filtros ───────────────────────────────────────────────
        patentes = conn.execute(
            "SELECT DISTINCT patente FROM flota "
            "WHERE patente != 'SIN SELECCIONAR' AND patente IS NOT NULL "
            "ORDER BY patente LIMIT 500"
        ).fetchall()
        talleres = conn.execute(
            "SELECT DISTINCT taller FROM flota "
            "WHERE taller != 'SIN SELECCIONAR' AND taller IS NOT NULL "
            "ORDER BY taller LIMIT 200"
        ).fetchall()

    finally:
        conn.close()

    return render_template(
        "index.html",
        data=data,
        kpis=kpis,
        patentes=[r[0] for r in patentes],
        talleres=[r[0] for r in talleres],
        filters={"patente": patente, "taller": taller, "alerta": alerta},
        pagination={
            "page": page,
            "total_pages": total_pages,
            "total_filtered": total_filtered,
            "page_size": PAGE_SIZE,
        },
    )