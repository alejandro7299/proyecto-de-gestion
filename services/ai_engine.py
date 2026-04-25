"""
Servicio de IA para análisis de flota
=====================================
Funcionalidades:
- Clasificación de alertas por severidad
- Detección de vehículos problemáticos
- Ranking de talleres
- Detección de anomalías de costo
- Análisis de frecuencia de fallas
"""
 
import sqlite3
from db import get_db
from collections import defaultdict
from datetime import datetime
 
 
# ── Clasificación de alertas ────────────────────────────────────────────────
 
CRITICAL_KEYWORDS = [
    "motor", "caja", "transmision", "freno", "dirección", "suspension",
    "accidente", "rotura", "grave", "urgente", "parado", "siniestro",
    "incendio", "fuga", "aceite", "refrigerante"
]
 
WARNING_KEYWORDS = [
    "desgaste", "preventivo", "revision", "cambio", "filtro",
    "correa", "bateria", "neumatico", "llanta", "amortiguador"
]
 
 
def classify_alert(row: dict) -> str:
    """
    Clasifica una fila como CRÍTICO, MEDIO u OK basado en:
    - Palabras clave en observacion/detalle/accion
    - Costo elevado
    - Rubro/subrubro
    """
    text = " ".join([
        str(row.get("observacion", "")),
        str(row.get("detalle", "")),
        str(row.get("accion", "")),
        str(row.get("rubro", "")),
    ]).lower()
 
    costo = float(row.get("costo", 0) or 0)
 
    if any(kw in text for kw in CRITICAL_KEYWORDS) or costo > 500_000:
        return "CRÍTICO"
    elif any(kw in text for kw in WARNING_KEYWORDS) or costo > 100_000:
        return "MEDIO"
    else:
        return "OK"
 
 
# ── Análisis de flota ───────────────────────────────────────────────────────
 
def get_fleet_analysis() -> dict:
    """
    Genera análisis completo de la flota:
    - Vehículos problemáticos
    - Ranking de talleres
    - Anomalías de costo
    - KPIs generales
    """
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM flota").fetchall()
        data = [dict(r) for r in rows]
    finally:
        conn.close()
 
    if not data:
        return {"error": "Sin datos"}
 
    return {
        "kpis": _compute_kpis(data),
        "problematic_vehicles": _detect_problematic_vehicles(data),
        "workshop_ranking": _rank_workshops(data),
        "cost_anomalies": _detect_cost_anomalies(data),
        "monthly_trend": _monthly_trend(data),
        "top_rubros": _top_rubros(data),
    }
 
 
def _compute_kpis(data: list) -> dict:
    total_cost = sum(float(r.get("costo", 0) or 0) for r in data)
    unique_vehicles = len({r["patente"] for r in data if r.get("patente") and r["patente"] != "SIN SELECCIONAR"})
    unique_workshops = len({r["taller"] for r in data if r.get("taller") and r["taller"] != "SIN SELECCIONAR"})
    avg_cost = total_cost / len(data) if data else 0
 
    critical_count = sum(1 for r in data if classify_alert(r) == "CRÍTICO")
 
    return {
        "total_registros": len(data),
        "costo_total": round(total_cost, 2),
        "costo_promedio": round(avg_cost, 2),
        "vehiculos_activos": unique_vehicles,
        "talleres_activos": unique_workshops,
        "alertas_criticas": critical_count,
    }
 
 
def _detect_problematic_vehicles(data: list, top_n: int = 10) -> list:
    """
    Detecta vehículos problemáticos por:
    - Costo acumulado alto
    - Frecuencia de intervenciones alta
    - Índice combinado: costo × frecuencia
    """
    vehicle_stats = defaultdict(lambda: {"costo": 0.0, "count": 0, "criticos": 0})
 
    for r in data:
        pat = r.get("patente", "SIN SELECCIONAR")
        if not pat or pat == "SIN SELECCIONAR":
            continue
        vehicle_stats[pat]["costo"] += float(r.get("costo", 0) or 0)
        vehicle_stats[pat]["count"] += 1
        if classify_alert(r) == "CRÍTICO":
            vehicle_stats[pat]["criticos"] += 1
 
    result = []
    for pat, stats in vehicle_stats.items():
        # Score compuesto: normalización simple
        score = (stats["costo"] / 1000) + (stats["count"] * 10) + (stats["criticos"] * 50)
        result.append({
            "patente": pat,
            "costo_total": round(stats["costo"], 2),
            "intervenciones": stats["count"],
            "alertas_criticas": stats["criticos"],
            "score_riesgo": round(score, 1),
        })
 
    result.sort(key=lambda x: x["score_riesgo"], reverse=True)
    return result[:top_n]
 
 
def _rank_workshops(data: list) -> list:
    """
    Ranking de talleres por:
    - Costo total facturado
    - Número de órdenes
    - Costo promedio por orden
    """
    workshop_stats = defaultdict(lambda: {"costo": 0.0, "count": 0})
 
    for r in data:
        taller = r.get("taller", "SIN SELECCIONAR")
        if not taller or taller == "SIN SELECCIONAR":
            continue
        workshop_stats[taller]["costo"] += float(r.get("costo", 0) or 0)
        workshop_stats[taller]["count"] += 1
 
    result = []
    for taller, stats in workshop_stats.items():
        avg = stats["costo"] / stats["count"] if stats["count"] > 0 else 0
        result.append({
            "taller": taller,
            "costo_total": round(stats["costo"], 2),
            "ordenes": stats["count"],
            "costo_promedio": round(avg, 2),
        })
 
    result.sort(key=lambda x: x["costo_total"], reverse=True)
    return result[:15]
 
 
def _detect_cost_anomalies(data: list, z_threshold: float = 2.0) -> list:
    """
    Detecta registros con costos anómalos usando Z-score.
    """
    costos = [float(r.get("costo", 0) or 0) for r in data]
    if not costos:
        return []
 
    mean = sum(costos) / len(costos)
    variance = sum((c - mean) ** 2 for c in costos) / len(costos)
    std = variance ** 0.5
 
    if std == 0:
        return []
 
    anomalies = []
    for r in data:
        costo = float(r.get("costo", 0) or 0)
        z = (costo - mean) / std
        if abs(z) >= z_threshold and costo > 0:
            anomalies.append({
                "id": r.get("id"),
                "patente": r.get("patente"),
                "taller": r.get("taller"),
                "costo": round(costo, 2),
                "z_score": round(z, 2),
                "detalle": r.get("detalle", ""),
                "fecha": r.get("fecha", ""),
            })
 
    anomalies.sort(key=lambda x: abs(x["z_score"]), reverse=True)
    return anomalies[:20]
 
 
def _monthly_trend(data: list) -> list:
    """Agrupa costos por mes."""
    monthly = defaultdict(float)
    for r in data:
        mes = r.get("mes", "")
        if mes and mes != "SIN SELECCIONAR":
            monthly[mes] += float(r.get("costo", 0) or 0)
 
    result = [{"mes": k, "costo": round(v, 2)} for k, v in monthly.items()]
    result.sort(key=lambda x: x["mes"])
    return result
 
 
def _top_rubros(data: list, top_n: int = 8) -> list:
    """Top rubros por costo total."""
    rubro_costs = defaultdict(float)
    for r in data:
        rubro = r.get("rubro", "SIN SELECCIONAR")
        rubro_costs[rubro] += float(r.get("costo", 0) or 0)
 
    result = [{"rubro": k, "costo": round(v, 2)} for k, v in rubro_costs.items()]
    result.sort(key=lambda x: x["costo"], reverse=True)
    return result[:top_n]
 
