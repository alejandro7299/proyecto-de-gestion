from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from db import get_db, USE_SQLITE

fleet_bp = Blueprint("fleet", __name__)


def _ph(sql):
    """Convierte ? a %s para PostgreSQL."""
    return sql if USE_SQLITE else sql.replace("?", "%s")


@fleet_bp.route("/registro", methods=["GET", "POST"])
def registro():
    if request.method == "POST":
        data = request.form.to_dict()
        conn = get_db()
        try:
            cols = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            cur = conn.cursor()
            cur.execute(_ph(f"INSERT INTO flota ({cols}) VALUES ({placeholders})"),
                        list(data.values()))
            cur.close()
            conn.commit()
            flash("✅ Registro guardado correctamente", "success")
        except Exception as e:
            flash(f"❌ Error al guardar: {e}", "danger")
        finally:
            conn.close()
        return redirect(url_for("main.index"))

    return render_template("registro.html")


@fleet_bp.route("/delete/<int:record_id>", methods=["POST"])
def delete(record_id):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("DELETE FROM flota WHERE id = ?"), (record_id,))
        cur.close()
        conn.commit()
        return jsonify({"success": True, "message": f"Registro #{record_id} eliminado"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


@fleet_bp.route("/edit/<int:record_id>", methods=["GET", "POST"])
def edit(record_id):
    conn = get_db()
    try:
        if request.method == "POST":
            data = request.form.to_dict()
            sets = ", ".join(f"{k} = ?" for k in data.keys())
            values = list(data.values()) + [record_id]
            cur = conn.cursor()
            cur.execute(_ph(f"UPDATE flota SET {sets} WHERE id = ?"), values)
            cur.close()
            conn.commit()
            flash("✅ Registro actualizado", "success")
            return redirect(url_for("main.index"))

        cur = conn.cursor()
        cur.execute(_ph("SELECT * FROM flota WHERE id = ?"), (record_id,))
        row = cur.fetchone()
        cur.close()

        if not row:
            flash("Registro no encontrado", "danger")
            return redirect(url_for("main.index"))

        return render_template("edit.html", row=dict(row))
    finally:
        conn.close()