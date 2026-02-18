import os
from groq import Groq
from flask import current_app
import logging
import traceback

class ChatManager:
    def __init__(self):
        self.model = None
        self._setup_done = False

    def setup(self):
        api_key = current_app.config.get("GROQ_API_KEY") or os.environ.get("GROQ_API_KEY")
        
        if not api_key:
            logging.error("AskVera: GROQ_API_KEY missing.")
            return

        try:
            self.model = Groq(api_key=api_key)
            self._setup_done = True
            logging.info("AskVera: Groq AI initialized successfully.")
        except Exception as e:
            logging.error(f"AskVera init failed: {e}")

    def get_response(self, user_message, context, user_identity, user_role="guest"):
        if not current_app.config.get("ENABLE_ASKVERA", False):
            return {"error": "AskVera is disabled.", "fallback": True}

        if not self.model: 
            self.setup()
        
        if not self.model: 
            return {"error": "AI service unavailable (Init failed).", "fallback": True}

        try:
            system_prompt = (
                f"You are AskVera, the official intelligent assistant of the SkillVerse platform. "
                f"Role: {user_role}. "
                f"Tone: Professional, Honest, Simple. "
                f"Context: {context.get('page', 'unknown')}. "
                f"AVAILABLE FEATURES: User management, Services listing, Categories, Orders, Bookings, Availability management. "
                f"RULES: "
                f"1. ONLY suggest/discuss existing features. Do NOT invent analytics, numbers, or future integrations. "
                f"2. If data is not provided, say 'Data not available yet'. If count is zero, say 'No records found yet'. "
                f"3. Do NOT repeat visible dashboard numbers. Explain HOW to find/use features instead. "
                f"4. Be helpful but strictly realistic about the platform capabilities."
            )
            
            response = self.model.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.3,
                max_tokens=300,
                top_p=0.9
            )
            
            ai_text = response.choices[0].message.content.strip()
            return {"response": ai_text, "suggestions": self.get_initial_suggestions(user_role)[:3]}
                
        except Exception as e:
            print(f"ChatManager Error: {e}")
            traceback.print_exc()
            return {"error": "I'm having trouble connecting right now.", "fallback": True}

    def get_initial_suggestions(self, role):
        if role == 'admin':
            return [
                "How can I manage users on this platform?",
                "How do I add or update services?",
                "How can I manage service categories?",
                "How do I view or handle orders?",
                "What should I configure first as an admin?"
            ]
        else: # Normal User (client/provider/guest)
            return [
                "How do I find the right service for me?",
                "How can I book a service?",
                "How do I manage my bookings?",
                "How can I contact a service provider?",
                "How do I track my orders?"
            ]

chat_manager = ChatManager()
