"""
下单条件判定配置路由
"""
from flask import Blueprint, jsonify, request
from services import order_gate_service

order_gate_bp = Blueprint('order_gate', __name__)


@order_gate_bp.route('/order_gate/config', methods=['GET'])
def get_order_gate_config():
    seed_raw = str(request.args.get('seed_if_empty', 'true')).strip().lower()
    seed_if_empty = seed_raw not in ('0', 'false', 'no', 'off')
    result = order_gate_service.get_config(seed_if_empty=seed_if_empty)
    status = 200 if result.get('success') else 500
    return jsonify(result), status


@order_gate_bp.route('/order_gate/config', methods=['PUT'])
def save_order_gate_config():
    payload = request.get_json(silent=True) or {}
    signals = payload.get('signals') or []
    combos = payload.get('combos') or []
    result = order_gate_service.save_config(signals=signals, combos=combos)
    return jsonify(result), (200 if result.get('success') else 400)


@order_gate_bp.route('/order_gate/config/reset_defaults', methods=['POST'])
def reset_order_gate_defaults():
    result = order_gate_service.reset_to_defaults()
    return jsonify(result), (200 if result.get('success') else 400)
