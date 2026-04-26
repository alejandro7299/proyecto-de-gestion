import os
import psycopg2
from psycopg2.extras import RealDictCursor

# En Render esta variable se configura automáticamente al conectar la DB.
# Localmente podés usar SQLite si no tenés DATABASE_URL seteada.
DATABASE_URL = os.environ.get("DATABASE_URL")

# ── Compatibilidad local: si no hay PostgreSQL usamos SQLite ─────────────────
USE_SQLITE = DATABASE_URL is None

if USE_SQLITE:
    import sqlite3
    _HERE = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.join(_HERE, "fleet.db")
    print(f"[DB] Modo LOCAL - SQLite: {DB_PATH}")
else:
    print(f"[DB] Modo PRODUCCION - PostgreSQL conectado")

# ── Schema ───────────────────────────────────────────────────────────────────
SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS flota (
    id SERIAL PRIMARY KEY,
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

SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS flota (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    patente TEXT, marca TEXT, modelo TEXT, comprobante TEXT,
    fecha TEXT, mes TEXT, observacion TEXT, detalle TEXT,
    accion TEXT, subrubro TEXT, insumo TEXT, nominsumo TEXT,
    taller TEXT, cantidad REAL, precio_unit REAL, costo REAL,
    centrocosto TEXT, operador TEXT, n_ot TEXT, panol TEXT,
    inicio_ot TEXT, tecnico TEXT, cumplida TEXT, fin_ot TEXT,
    operacion TEXT, fletero TEXT, empresa TEXT, rubro TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_patente ON flota(patente);
CREATE INDEX IF NOT EXISTS idx_taller ON flota(taller);
CREATE INDEX IF NOT EXISTS idx_fecha ON flota(fecha);
CREATE INDEX IF NOT EXISTS idx_rubro ON flota(rubro);
"""


# ── Conexión ─────────────────────────────────────────────────────────────────

def get_db():
    if USE_SQLITE:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    else:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        conn.autocommit = False
        return conn


def init_db():
    if USE_SQLITE:
        import sqlite3
        conn = get_db()
        conn.executescript(SCHEMA_SQLITE)
        conn.commit()
        conn.close()
    else:
        conn = get_db()
        cur = conn.cursor()
        # Ejecutar cada statement por separado en PostgreSQL
        for statement in SCHEMA_PG.strip().split(";"):
            s = statement.strip()
            if s:
                try:
                    cur.execute(s)
                except Exception as e:
                    print(f"[DB] Schema warning: {e}")
        conn.commit()
        cur.close()
        conn.close()


def add_column_if_missing(conn, table: str, col_name: str, col_type: str = "TEXT"):
    """Agrega columna si no existe — compatible con SQLite y PostgreSQL."""
    if USE_SQLITE:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        existing = {row["name"].lower() for row in cursor.fetchall()}
        if col_name.lower() not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
            conn.commit()
    else:
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
        """, (table, col_name.lower()))
        if not cur.fetchone():
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                conn.commit()
            except Exception as e:
                print(f"[DB] add_column warning: {e}")
        cur.close()