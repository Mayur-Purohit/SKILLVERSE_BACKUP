from flask import Blueprint, request, jsonify, current_app
# NOTE: If not using Flask-Login, adjust the following import and user logic
from flask_login import current_user
from chat_manager import chat_manager

chat_bp = Blueprint('chat', __name__)

@chat_bp.route('/ask', methods=['POST'])
def ask():
    if not current_app.config.get('ENABLE_ASKVERA'):
        return jsonify({"error": "Feature disabled"}), 404
        
    data = request.get_json()
    if not data or 'message' not in data:
        return jsonify({"error": "Message required"}), 400
        
    user_message = data['message']
    context = data.get('context', {})
    
    # Determine Identity (Edit this if your User model differs)
    if hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
        user_identity = f"user_{current_user.id}"
        # Adjust 'is_provider' check to match your User model
        if hasattr(current_user, 'is_provider') and current_user.is_provider():
            user_role = 'provider'
        elif hasattr(current_user, 'is_admin') and current_user.is_admin():
            user_role = 'admin'
        else:
            user_role = 'client'
    else:
        user_identity = f"ip_{request.remote_addr}"
        user_role = 'guest'
        
    result = chat_manager.get_response(user_message, context, user_identity, user_role)
    return jsonify(result)

@chat_bp.route('/init', methods=['GET'])
def init_chat():
    if not current_app.config.get('ENABLE_ASKVERA'):
        return jsonify({"error": "Feature disabled"}), 404

    # Reuse role logic (ideal to refactor, but keeping inline to minimize diffs)
    if hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
        if hasattr(current_user, 'is_admin') and current_user.is_admin():
            user_role = 'admin'
        elif hasattr(current_user, 'is_provider') and current_user.is_provider():
            user_role = 'provider'
        else:
            user_role = 'client'
    else:
        user_role = 'guest'
        
    suggestions = chat_manager.get_initial_suggestions(user_role)
    return jsonify({"suggestions": suggestions})
