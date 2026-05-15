"""
不可买原因维护路由
"""
from flask import Blueprint, jsonify, request
from services import no_buy_reason_service

no_buy_reason_bp = Blueprint('no_buy_reason', __name__)


@no_buy_reason_bp.route('/no_buy_reasons', methods=['GET'])
def list_no_buy_reasons():
    active_only_raw = str(request.args.get('active_only', 'false')).strip().lower()
    active_only = active_only_raw in ('1', 'true', 'yes', 'on')
    rows = no_buy_reason_service.list_reasons(active_only=active_only)
    return jsonify({'success': True, 'data': rows, 'count': len(rows)})


@no_buy_reason_bp.route('/no_buy_reasons', methods=['POST'])
def create_no_buy_reason():
    payload = request.get_json(silent=True) or {}
    result = no_buy_reason_service.create_reason(payload)
    return jsonify(result), (201 if result.get('success') else 400)


@no_buy_reason_bp.route('/no_buy_reasons/<int:reason_id>', methods=['PUT'])
def update_no_buy_reason(reason_id):
    payload = request.get_json(silent=True) or {}
    result = no_buy_reason_service.update_reason(reason_id, payload)
    return jsonify(result), (200 if result.get('success') else 400)


@no_buy_reason_bp.route('/no_buy_reasons/<int:reason_id>', methods=['DELETE'])
def delete_no_buy_reason(reason_id):
    result = no_buy_reason_service.delete_reason(reason_id)
    return jsonify(result), (200 if result.get('success') else 400)


@no_buy_reason_bp.route('/no_buy_reasons/delete_logs', methods=['POST'])
def create_no_buy_reason_delete_log():
    payload = request.get_json(silent=True) or {}
    result = no_buy_reason_service.create_delete_log(payload)
    return jsonify(result), (201 if result.get('success') else 400)

