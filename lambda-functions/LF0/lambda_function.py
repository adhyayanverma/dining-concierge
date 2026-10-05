# LF0 - Phase 3: API Gateway -> LF0 -> Lex V2 -> back to the frontend
# Needs environment variables: BOT_ID, BOT_ALIAS_ID, LOCALE_ID (en_US)
# Input  (BotRequest):  {"messages": [{"type": "unstructured", "unstructured": {"id": "<sessionId>", "text": "..."}}]}
# Output (BotResponse): {"messages": [{"type": "unstructured", "unstructured": {"id": "...", "text": "...", "timestamp": "..."}}]}

import json
import os
import re
import uuid
from datetime import datetime, timezone

import boto3

lex = boto3.client("lexv2-runtime")

BOT_ID = os.environ.get("BOT_ID")
BOT_ALIAS_ID = os.environ.get("BOT_ALIAS_ID")
LOCALE_ID = os.environ.get("LOCALE_ID", "en_US")

# Lex session IDs may only contain letters, digits, and . _ : -  (2-100 characters)
SESSION_ID_PATTERN = re.compile(r"^[0-9a-zA-Z._:-]{2,100}$")


def build_response(texts):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {"id": str(uuid.uuid4()), "text": text, "timestamp": now},
            }
            for text in texts
        ]
    }


def lambda_handler(event, context):
    print("Incoming event:", json.dumps(event))  # visible in CloudWatch Logs

    # 1. Extract the text message (and session ID) from the API request
    try:
        unstructured = event["messages"][0]["unstructured"]
        user_text = unstructured["text"].strip()
    except (KeyError, IndexError, TypeError, AttributeError):
        return build_response(["Sorry, I couldn't read your message. Please try again."])

    if not user_text:
        return build_response(["Please type a message."])

    session_id = unstructured.get("id") or "default-session"
    if not SESSION_ID_PATTERN.match(session_id):
        session_id = "default-session"

    # 2-3. Send it to Lex and wait for the response
    try:
        lex_response = lex.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=user_text,
        )
    except Exception as error:
        print("Lex error:", repr(error))
        return build_response(["Sorry, I'm having trouble right now. Please try again in a moment."])

    print("Lex response:", json.dumps(lex_response, default=str))

    # 4. Send Lex's reply back as the API response
    texts = [m["content"] for m in lex_response.get("messages", []) if m.get("content")]
    if not texts:
        texts = ["Sorry, I didn't catch that. Could you rephrase?"]

    return build_response(texts)
