import sqlite3
import os

# CRITICAL: Always resolve DB path relative to THIS file's directory,
# using abspath to survive any working-directory changes.
# If you move the DB, set the FLEET_DB env variable instead.
_HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("FLEET_DB", os.path.join(_HERE, "fleet.db"))

# Log the path at import time so you can see it in the terminal
print(f"[DB] Using database: {DB_PATH}")

SCHEMA = """
CREATE TABLE IF NOT EXISTS flota (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    patente TEXT,
    marca TEXT,
    modelo TEXT,
    comprobante TEXT,
    fecha TEXT,
    mes TEXT,
    observacion TEXT,
    detalle TEXT,
    accion TEXT,
    subrubro TEXT,
    insumo TEXT,
    nominsumo TEXT,
    taller TEXT,
    cantidad REAL,
    precio_unit REAL,
    costo REAL,
    centrocosto TEXT,
    operador TEXT,
    n_ot TEXT,
    panol TEXT,
    inicio_ot TEXT,
    tecnico TEXT,
    cumplida TEXT,
    fin_ot TEXT,
    operacion TEXT,
    fletero TEXT,
    empresa TEXT,
    rubro TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_patente ON flota(patente);
CREATE INDEX IF NOT EXISTS idx_taller ON flota(taller);
CREATE INDEX IF NOT EXISTS idx_fecha ON flota(fecha);
CREATE INDEX IF NOT EXISTS idx_rubro ON flota(rubro);
"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


def add_column_if_missing(conn, table: str, col_name: str, col_type: str = "TEXT"):
    """Dynamically add a column to a table if it doesn't exist yet."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    existing = {row["name"].lower() for row in cursor.fetchall()}
    if col_name.lower() not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
        conn.commit()