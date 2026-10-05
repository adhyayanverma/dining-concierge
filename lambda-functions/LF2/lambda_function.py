# LF2 - Queue worker for the Dining Concierge
# Every run: pull requests from SQS (Q1) -> pick random restaurants for the cuisine
# -> get details from DynamoDB -> email them with SES -> delete the message from the queue.
#
# Environment variables:
#   QUEUE_URL         SQS Q1 URL                                   (required)
#   SENDER_EMAIL      SES-verified "from" address                  (required)
#   TABLE_NAME        DynamoDB table, default "yelp-restaurants"
#   NUM_SUGGESTIONS   how many restaurants per email, default 3
#   STATE_TABLE       DynamoDB table for each user's last search, e.g. "user-state"  (extra credit)
#   --- added in Phase 6; until then restaurant IDs come from DynamoDB ---
#   OPENSEARCH_ENDPOINT  e.g. https://search-xxxx.us-east-1.es.amazonaws.com
#   OPENSEARCH_USER / OPENSEARCH_PASSWORD   master user from domain creation
#   OPENSEARCH_INDEX     default "restaurants"

import base64
import json
import os
import random
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

sqs = boto3.client("sqs")
ses = boto3.client("ses")
dynamodb = boto3.resource("dynamodb")

QUEUE_URL = os.environ.get("QUEUE_URL")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
TABLE_NAME = os.environ.get("TABLE_NAME", "yelp-restaurants")
NUM_SUGGESTIONS = int(os.environ.get("NUM_SUGGESTIONS", "3"))
OPENSEARCH_ENDPOINT = (os.environ.get("OPENSEARCH_ENDPOINT") or "").rstrip("/")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER", "")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD", "")
OPENSEARCH_INDEX = os.environ.get("OPENSEARCH_INDEX", "restaurants")
STATE_TABLE = os.environ.get("STATE_TABLE")
NY_TZ = ZoneInfo("America/New_York")


# ---------- step 1: pick random restaurant IDs for a cuisine ----------

def random_ids_from_opensearch(cuisine, count):
    body = {
        "size": count,
        "_source": ["RestaurantID"],
        "query": {
            "function_score": {
                "query": {"term": {"Cuisine": cuisine}},
                "random_score": {},
                "boost_mode": "replace",
            }
        },
    }
    request = urllib.request.Request(
        f"{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_search",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    credentials = base64.b64encode(f"{OPENSEARCH_USER}:{OPENSEARCH_PASSWORD}".encode()).decode()
    request.add_header("Authorization", f"Basic {credentials}")
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.loads(response.read())
    return [hit["_source"]["RestaurantID"] for hit in result["hits"]["hits"]]


def random_ids_from_dynamodb(cuisine, count):
    # Temporary source used until OpenSearch is set up in Phase 6
    table = dynamodb.Table(TABLE_NAME)
    ids, kwargs = [], {"FilterExpression": Attr("Cuisine").eq(cuisine), "ProjectionExpression": "BusinessID"}
    while True:
        page = table.scan(**kwargs)
        ids.extend(item["BusinessID"] for item in page["Items"])
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return random.sample(ids, min(count, len(ids)))


def pick_restaurant_ids(cuisine, count):
    if OPENSEARCH_ENDPOINT:
        print(f"Picking {count} random '{cuisine}' restaurants from OpenSearch")
        return random_ids_from_opensearch(cuisine, count)
    print(f"Picking {count} random '{cuisine}' restaurants from DynamoDB (OpenSearch not configured yet)")
    return random_ids_from_dynamodb(cuisine, count)


# ---------- step 2: get restaurant details from DynamoDB ----------

def get_restaurant_details(ids):
    if not ids:
        return []
    response = dynamodb.batch_get_item(
        RequestItems={TABLE_NAME: {"Keys": [{"BusinessID": i} for i in ids]}}
    )
    by_id = {item["BusinessID"]: item for item in response["Responses"].get(TABLE_NAME, [])}
    return [by_id[i] for i in ids if i in by_id]  # keep the random order


# ---------- step 3: format the email ----------

def friendly_date(date_str):
    try:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return date_str or "your chosen date"
    today = datetime.now(NY_TZ).date()
    if day == today:
        return "today"
    if day == today + timedelta(days=1):
        return "tomorrow"
    return f"{day.strftime('%A, %B')} {day.day}"


def friendly_time(time_str):
    try:
        t = datetime.strptime(time_str, "%H:%M")
    except (TypeError, ValueError):
        return time_str or "your chosen time"
    hour = t.hour % 12 or 12
    return f"{hour}:{t.minute:02d} {'AM' if t.hour < 12 else 'PM'}"


def restaurant_line(n, r):
    line = f"{n}. {r['Name']}, located at {r['Address']}"
    if "Rating" in r:
        line += f" (rated {r['Rating']}/5 from {r.get('NumberOfReviews', 0)} reviews)"
    return line


def build_repeat_email(request, restaurants):
    cuisine = request["cuisine"].title()
    location = str(request.get("location", "")).title()
    lines = [f"Welcome back! Here are the same {cuisine} restaurant suggestions in {location} as last time:", ""]
    lines += [restaurant_line(n, r) for n, r in enumerate(restaurants, 1)]
    lines += ["", "Enjoy your meal!"]
    return f"Your {cuisine} restaurant suggestions (same as last time)", "\n".join(lines)


def build_email(request, restaurants):
    cuisine = request["cuisine"].title()
    people = request.get("numberOfPeople")
    party = f"{people} {'person' if str(people) == '1' else 'people'}" if people else "your party"
    when = f"{friendly_date(request.get('diningDate'))} at {friendly_time(request.get('diningTime'))}"

    lines = [f"Hello! Here are my {cuisine} restaurant suggestions for {party}, for {when}:", ""]
    lines += [restaurant_line(n, r) for n, r in enumerate(restaurants, 1)]
    lines += ["", "Enjoy your meal!"]
    subject = f"Your {cuisine} restaurant suggestions"
    return subject, "\n".join(lines)


# ---------- main ----------

def process(message):
    request = json.loads(message["Body"])
    print("Processing request:", json.dumps(request))

    if request.get("repeat") and request.get("restaurantIds"):
        ids = request["restaurantIds"]
        print("Repeat request: reusing last time's restaurants", ids)
    else:
        ids = pick_restaurant_ids(request["cuisine"], NUM_SUGGESTIONS)
    restaurants = get_restaurant_details(ids)
    if not restaurants:
        raise RuntimeError(f"No restaurants found for cuisine '{request['cuisine']}'")

    if request.get("repeat"):
        subject, body = build_repeat_email(request, restaurants)
    else:
        subject, body = build_email(request, restaurants)
    print("Email body:\n" + body)
    ses.send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [request["email"]]},
        Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
    )
    print(f"Email sent to {request['email']}")
    save_last_search(request, [r["BusinessID"] for r in restaurants])


def save_last_search(request, restaurant_ids):
    # Extra credit: remember this search so the bot can offer it again next time
    if not STATE_TABLE or not request.get("userId"):
        return
    try:
        dynamodb.Table(STATE_TABLE).put_item(Item={
            "userId": request["userId"],
            "location": request.get("location"),
            "cuisine": request["cuisine"],
            "email": request["email"],
            "restaurantIds": restaurant_ids,
            "updatedAt": datetime.now(NY_TZ).isoformat(),
        })
        print(f"Saved last search for user {request['userId']}")
    except Exception as error:
        # The email already went out, so don't fail the whole message over this
        print("Could not save last search:", repr(error))


def lambda_handler(event, context):
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
        VisibilityTimeout=120,  # hide messages while we work on them
    )
    messages = response.get("Messages", [])
    print(f"Received {len(messages)} message(s) from SQS")

    sent = 0
    for message in messages:
        try:
            process(message)
            sent += 1
            delete = True
        except ClientError as error:
            code = error.response["Error"]["Code"]
            print(f"AWS error ({code}):", error)
            # MessageRejected = unverified address in SES sandbox; retrying will never work, so drop it
            delete = code == "MessageRejected"
        except Exception as error:
            print("Error processing message (it will be retried later):", repr(error))
            delete = False

        if delete:
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])

    return {"received": len(messages), "emailsSent": sent}
