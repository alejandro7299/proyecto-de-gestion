from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from db import get_db
 
fleet_bp = Blueprint("fleet", __name__)
 
 
@fleet_bp.route("/registro", methods=["GET", "POST"])
def registro():
    if request.method == "POST":
        data = request.form.to_dict()
        conn = get_db()
        try:
            cols = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            conn.execute(
                f"INSERT INTO flota ({cols}) VALUES ({placeholders})",
                list(data.values())
            )
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
        conn.execute("DELETE FROM flota WHERE id = ?", (record_id,))
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
            conn.execute(f"UPDATE flota SET {sets} WHERE id = ?", values)
            conn.commit()
            flash("✅ Registro actualizado", "success")
            return redirect(url_for("main.index"))
 
        row = conn.execute("SELECT * FROM flota WHERE id = ?", (record_id,)).fetchone()
        if not row:
            flash("Registro no encontrado", "danger")
            return redirect(url_for("main.index"))
 
        return render_template("edit.html", row=dict(row))
    finally:
        conn.close()
 