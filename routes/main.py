from flask import Blueprint, render_template, request, jsonify
from db import get_db, USE_SQLITE
from services.ai_engine import classify_alert

main_bp = Blueprint("main", __name__)

PAGE_SIZE = 200


def _execute(conn, sql, params=None):
    """Ejecuta una query compatible con SQLite (?), y PostgreSQL (%s)."""
    if not USE_SQLITE:
        sql = sql.replace("?", "%s")
    cur = conn.cursor()
    cur.execute(sql, params or [])
    return cur


@main_bp.route("/")
def index():
    conn = get_db()
    try:
        patente = request.args.get("patente", "").strip()
        taller  = request.args.get("taller",  "").strip()
        alerta  = request.args.get("alerta",  "").strip()
        page    = max(1, int(request.args.get("page", 1)))

        # ── KPIs sobre toda la tabla ─────────────────────────────────────────
        cur = _execute(conn, """
            SELECT
                COUNT(*)                 AS total,
                COALESCE(SUM(costo), 0)  AS costo_total,
                COUNT(DISTINCT patente)  AS vehiculos,
                COUNT(DISTINCT taller)   AS talleres
            FROM flota
        """)
        kpi_row = cur.fetchone()
        cur.close()

        # psycopg2 RealDictRow o sqlite3.Row — ambos soportan acceso por clave
        kpis = {
            "total":       kpi_row["total"],
            "costo_total": float(kpi_row["costo_total"] or 0),
            "vehiculos":   kpi_row["vehiculos"],
            "talleres":    kpi_row["talleres"],
        }

        # ── Filtros ──────────────────────────────────────────────────────────
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

        cur = _execute(conn, f"SELECT COUNT(*) AS n FROM flota WHERE {where_clause}", params)
        count_row = cur.fetchone()
        cur.close()
        total_filtered = count_row["n"]
        total_pages = max(1, (total_filtered + PAGE_SIZE - 1) // PAGE_SIZE)

        cur = _execute(conn,
            f"SELECT * FROM flota WHERE {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [PAGE_SIZE, offset]
        )
        data = [dict(r) for r in cur.fetchall()]
        cur.close()

        # ── Clasificación IA ─────────────────────────────────────────────────
        for row in data:
            row["alerta_ia"] = classify_alert(row)

        if alerta:
            data = [r for r in data if r["alerta_ia"] == alerta]

        criticos = sum(1 for r in data if r["alerta_ia"] == "CRÍTICO")
        kpis["criticos"] = criticos

        # ── Opciones de filtros ───────────────────────────────────────────────
        cur = _execute(conn,
            "SELECT DISTINCT patente FROM flota "
            "WHERE patente != 'SIN SELECCIONAR' AND patente IS NOT NULL "
            "ORDER BY patente LIMIT 500"
        )
        patentes = [r["patente"] for r in cur.fetchall()]
        cur.close()

        cur = _execute(conn,
            "SELECT DISTINCT taller FROM flota "
            "WHERE taller != 'SIN SELECCIONAR' AND taller IS NOT NULL "
            "ORDER BY taller LIMIT 200"
        )
        talleres = [r["taller"] for r in cur.fetchall()]
        cur.close()

    finally:
        conn.close()

    return render_template(
        "index.html",
        data=data,
        kpis=kpis,
        patentes=patentes,
        talleres=talleres,
        filters={"patente": patente, "taller": taller, "alerta": alerta},
        pagination={
            "page": page,
            "total_pages": total_pages,
            "total_filtered": total_filtered,
            "page_size": PAGE_SIZE,
        },
    )