# LF1 - Lex V2 code hook for the Dining Concierge bot
# Handles: GreetingIntent, ThankYouIntent, DiningSuggestionsIntent
# Environment variables:
#   QUEUE_URL    URL of SQS queue Q1                               (required)
#   STATE_TABLE  DynamoDB table with each user's last search, e.g. "user-state"  (extra credit)

import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
QUEUE_URL = os.environ.get("QUEUE_URL")
STATE_TABLE = os.environ.get("STATE_TABLE")
NY_TZ = ZoneInfo("America/New_York")

# Restaurant data covers Manhattan only
VALID_LOCATIONS = {"manhattan", "new york", "new york city", "nyc", "ny", "brooklyn", "queens", "bronx", "the bronx", "staten island"}

# These MUST match the cuisines loaded into DynamoDB / OpenSearch in Phase 4
VALID_CUISINES = {"chinese", "italian", "japanese", "indian", "mexican", "thai"}

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------- helpers ----------

def get_slot(slots, name):
    """Return the slot's interpreted value (or what the user typed), or None if empty."""
    slot = slots.get(name) if slots else None
    if slot and slot.get("value"):
        value = slot["value"]
        return value.get("interpretedValue") or value.get("originalValue")
    return None


def text_message(text):
    return [{"contentType": "PlainText", "content": text}]


def close(event, intent, text, state="Fulfilled"):
    intent["state"] = state
    return {
        "sessionState": {
            "sessionAttributes": event["sessionState"].get("sessionAttributes", {}),
            "dialogAction": {"type": "Close"},
            "intent": intent,
        },
        "messages": text_message(text),
    }


def elicit_slot(event, intent, slot_name, text):
    intent["slots"][slot_name] = None  # clear the bad value so Lex asks again
    intent["state"] = "InProgress"
    return {
        "sessionState": {
            "sessionAttributes": event["sessionState"].get("sessionAttributes", {}),
            "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot_name},
            "intent": intent,
        },
        "messages": text_message(text),
    }


def delegate(event, intent):
    return {
        "sessionState": {
            "sessionAttributes": event["sessionState"].get("sessionAttributes", {}),
            "dialogAction": {"type": "Delegate"},
            "intent": intent,
        }
    }


# ---------- extra credit: remember the last search ----------

def user_id_from(event):
    # The frontend sends sessionId = "<userId>.<random>"; the userId part stays the same across visits
    return (event.get("sessionId") or "").split(".")[0]


def get_last_search(user_id):
    if not STATE_TABLE or not user_id:
        return None
    try:
        return dynamodb.Table(STATE_TABLE).get_item(Key={"userId": user_id}).get("Item")
    except Exception as error:
        print("Could not read last search:", repr(error))
        return None


def is_same_search(last, location, cuisine):
    return (
        last is not None
        and last.get("restaurantIds")
        and str(last.get("location", "")).strip().lower() == location.strip().lower()
        and str(last.get("cuisine", "")).strip().lower() == cuisine.strip().lower()
    )


def check_repeat_search(event, intent, slots):
    """Returns a Lex response if the 'same as last time?' logic should take over, else None."""
    location, cuisine = get_slot(slots, "Location"), get_slot(slots, "Cuisine")
    if not location or not cuisine:
        return None

    attributes = event["sessionState"].get("sessionAttributes") or {}
    event["sessionState"]["sessionAttributes"] = attributes
    user_id = user_id_from(event)
    answer = get_slot(slots, "SameAsLast")

    # 1. First time Location + Cuisine match the last search: ask the question
    if answer is None and attributes.get("repeatAsked") != "true":
        last = get_last_search(user_id)
        if is_same_search(last, location, cuisine):
            attributes["repeatAsked"] = "true"
            return elicit_slot(
                event, intent, "SameAsLast",
                f"Welcome back! Last time you also asked for {cuisine.strip().title()} food in "
                f"{location.strip().title()}. Would you like the same recommendations as last time? (yes or no)")
        return None

    # 2. User said yes: resend last time's restaurants to last time's email
    if answer and answer.strip().lower() == "yes":
        last = get_last_search(user_id)
        if is_same_search(last, location, cuisine):
            request = {
                "repeat": True,
                "userId": user_id,
                "location": last["location"],
                "cuisine": last["cuisine"],
                "email": last["email"],
                "restaurantIds": list(last["restaurantIds"]),
                "sessionId": event.get("sessionId"),
            }
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(request))
            print("Sent repeat request to SQS:", json.dumps(request))
            return close(event, intent,
                         f"Great! I'll send the same {cuisine.strip().title()} recommendations to "
                         f"{last['email']} shortly. Have a good day!")

    # 3. User said no (or anything else): continue the normal conversation
    return None


# ---------- validation ----------

def validate(slots):
    """Return (slot_name, error_message) for the first invalid slot, or None if all filled slots are valid."""
    location = get_slot(slots, "Location")
    if location and location.strip().lower() not in VALID_LOCATIONS:
        return ("Location",
                f"Sorry, I can't fulfill requests for {location}. I currently only cover New York City. "
                "Please enter a valid location.")

    cuisine = get_slot(slots, "Cuisine")
    if cuisine and cuisine.strip().lower() not in VALID_CUISINES:
        options = ", ".join(sorted(c.title() for c in VALID_CUISINES))
        return ("Cuisine",
                f"Sorry, I don't have suggestions for {cuisine} yet. Please choose one of: {options}.")

    people = get_slot(slots, "NumberOfPeople")
    if people:
        try:
            n = int(float(people))
            if n < 1 or n > 20:
                return ("NumberOfPeople", "I can book for 1 to 20 people. How many people are in your party?")
        except ValueError:
            return ("NumberOfPeople", "Please enter the number of people as a number, like 2.")

    now_ny = datetime.now(NY_TZ)
    dining_date = None
    date_str = get_slot(slots, "DiningDate")
    if date_str:
        try:
            dining_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return ("DiningDate", "I didn't understand that date. Please say something like 'today' or 'October 10'.")
        if dining_date < now_ny.date():
            return ("DiningDate", "That date is in the past. What date would you like to dine?")

    time_str = get_slot(slots, "DiningTime")
    if time_str:
        try:
            dining_time = datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            # Lex returns codes like "EV" for vague answers such as "evening"
            return ("DiningTime", "Please give me a specific time, like 7 pm.")
        if dining_date and dining_date == now_ny.date() and dining_time <= now_ny.time():
            return ("DiningTime", "That time has already passed today. What time would you like?")

    email = get_slot(slots, "Email")
    if email and not EMAIL_REGEX.match(email.strip()):
        return ("Email", "That doesn't look like a valid email address. Please enter it again.")

    return None


# ---------- main handler ----------

def lambda_handler(event, context):
    print("Incoming event:", json.dumps(event))  # visible in CloudWatch Logs

    intent = event["sessionState"]["intent"]
    intent_name = intent["name"]
    source = event.get("invocationSource")

    if intent_name == "GreetingIntent":
        return close(event, intent, "Hi there, how can I help?")

    if intent_name == "ThankYouIntent":
        return close(event, intent, "You're welcome! Have a great meal.")

    if intent_name == "DiningSuggestionsIntent":
        slots = intent["slots"]

        if source == "DialogCodeHook":
            problem = validate(slots)
            if problem:
                slot_name, error_message = problem
                return elicit_slot(event, intent, slot_name, error_message)
            repeat_response = check_repeat_search(event, intent, slots)
            if repeat_response:
                return repeat_response
            return delegate(event, intent)  # let Lex ask for the next empty slot

        if source == "FulfillmentCodeHook":
            request = {
                "location": get_slot(slots, "Location"),
                "cuisine": get_slot(slots, "Cuisine").strip().lower(),
                "numberOfPeople": get_slot(slots, "NumberOfPeople"),
                "diningDate": get_slot(slots, "DiningDate"),
                "diningTime": get_slot(slots, "DiningTime"),
                "email": get_slot(slots, "Email").strip(),
                "sessionId": event.get("sessionId"),
                "userId": user_id_from(event),
            }
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(request))
            print("Sent to SQS:", json.dumps(request))

            return close(event, intent,
                         f"You're all set. I'll email my {request['cuisine'].title()} restaurant suggestions "
                         f"to {request['email']} shortly. Have a good day!")

    return close(event, intent, "Sorry, I didn't catch that. Could you rephrase?", state="Failed")
