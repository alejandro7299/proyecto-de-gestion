from flask import Flask
from routes.main import main_bp
from routes.fleet import fleet_bp
from routes.import_export import import_export_bp
from routes.dashboard import dashboard_bp
from db import init_db

app = Flask(__name__)
app.secret_key = "flota_secreto_2024"
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max upload
 
# Register blueprints
app.register_blueprint(main_bp)
app.register_blueprint(fleet_bp)
app.register_blueprint(import_export_bp)
app.register_blueprint(dashboard_bp)
 
# ── Crear tablas siempre al arrancar ─────────────────────────────────────────
# Se llama a nivel de módulo para que funcione con `python app.py`,
# `flask run`, Gunicorn, y el reloader de debug mode.
with app.app_context():
    init_db()
    print(f"[DB] Tabla 'flota' verificada OK")
 
if __name__ == "__main__":
    app.run(debug=True, port=5000)
 