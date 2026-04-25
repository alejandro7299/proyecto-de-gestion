from flask import Blueprint, render_template, jsonify
from services.ai_engine import get_fleet_analysis

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/dashboard")
def dashboard():
    analysis = get_fleet_analysis()
    return render_template("dashboard.html", analysis=analysis)


@dashboard_bp.route("/api/analysis")
def api_analysis():
    """Endpoint JSON para actualización dinámica del dashboard."""
    return jsonify(get_fleet_analysis())