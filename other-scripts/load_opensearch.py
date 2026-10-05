"""
Phase 6 - Load restaurants into OpenSearch.

Creates index "restaurants" and stores, for every restaurant in DynamoDB "yelp-restaurants":
    {"RestaurantID": ..., "Cuisine": ..., "type": "Restaurant"}

OpenSearch 7+ removed mapping types, so the "Restaurant" type from the assignment is stored
as a field ("type": "Restaurant") on every document.

Run in AWS CloudShell:
    python3 load_opensearch.py
It will ask for the domain endpoint, master username and password.
"""

import base64
import getpass
import json
import urllib.error
import urllib.parse
import urllib.request

import boto3

REGION = "us-east-1"
TABLE_NAME = "yelp-restaurants"
INDEX = "restaurants"
BATCH_SIZE = 500

MAPPING = {
    "mappings": {
        "properties": {
            "RestaurantID": {"type": "keyword"},
            "Cuisine": {"type": "keyword"},
            "type": {"type": "keyword"},
        }
    }
}


class OpenSearch:
    def __init__(self, endpoint, user, password):
        endpoint = endpoint.strip()
        if not endpoint.startswith(("https://", "http://")):
            endpoint = "https://" + endpoint
        parts = urllib.parse.urlsplit(endpoint)
        # Keep only scheme + host, so a pasted Dashboards URL (".../_dashboards/...") still works
        self.endpoint = f"{parts.scheme}://{parts.netloc}"
        print(f"  Using endpoint: {self.endpoint}")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.auth = f"Basic {token}"

    def call(self, method, path, body=None, content_type="application/json"):
        data = None
        if body is not None:
            data = body.encode("utf-8") if isinstance(body, str) else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(self.endpoint + path, data=data, method=method)
        request.add_header("Authorization", self.auth)
        request.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, self.parse(response.read())
        except urllib.error.HTTPError as error:
            return error.code, self.parse(error.read())

    @staticmethod
    def parse(raw):
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            # Not JSON (usually an HTML page): show the start so we can see what answered
            return {"non_json_response": raw[:300].decode("utf-8", "replace")}


def read_restaurants_from_dynamodb():
    table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
    items, kwargs = [], {"ProjectionExpression": "BusinessID, Cuisine"}
    while True:
        page = table.scan(**kwargs)
        items.extend(page["Items"])
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return items


def main():
    endpoint = input("OpenSearch domain endpoint (starts with https://search-): ")
    user = input("Master username: ")
    password = getpass.getpass("Master password (typing is hidden): ")
    client = OpenSearch(endpoint, user, password)

    print("\nStep 1/4: checking the connection...")
    status, info = client.call("GET", "/")
    if status != 200 or "version" not in info:
        print(f"  FAILED (HTTP {status}): {info}")
        print("  Check the endpoint, username and password, then run the script again.")
        return
    print(f"  Connected to OpenSearch {info.get('version', {}).get('number', '?')}")

    print(f"Step 2/4: creating index '{INDEX}'...")
    status, result = client.call("PUT", f"/{INDEX}", MAPPING)
    if status == 200:
        print("  Index created")
    elif "resource_already_exists_exception" in json.dumps(result):
        print("  Index already exists, continuing")
    else:
        print(f"  FAILED (HTTP {status}): {result}")
        return

    print(f"Step 3/4: reading restaurants from DynamoDB '{TABLE_NAME}'...")
    items = read_restaurants_from_dynamodb()
    print(f"  Found {len(items)} restaurants")

    print("Step 4/4: loading them into OpenSearch...")
    loaded = 0
    for start in range(0, len(items), BATCH_SIZE):
        batch = items[start:start + BATCH_SIZE]
        lines = []
        for item in batch:
            lines.append(json.dumps({"index": {"_index": INDEX, "_id": item["BusinessID"]}}))
            lines.append(json.dumps({"RestaurantID": item["BusinessID"], "Cuisine": item["Cuisine"], "type": "Restaurant"}))
        status, result = client.call("POST", "/_bulk", "\n".join(lines) + "\n", "application/x-ndjson")
        if status != 200 or result.get("errors"):
            print(f"  FAILED on batch starting at {start} (HTTP {status}): {json.dumps(result)[:500]}")
            return
        loaded += len(batch)
        print(f"  loaded {loaded}/{len(items)}")

    client.call("POST", f"/{INDEX}/_refresh")
    status, count = client.call("GET", f"/{INDEX}/_count")
    print(f"\nDocuments in '{INDEX}': {count.get('count')}  (should equal {len(items)})")

    print("Per cuisine:")
    for cuisine in sorted({item["Cuisine"] for item in items}):
        _, c = client.call("POST", f"/{INDEX}/_count", {"query": {"term": {"Cuisine": cuisine}}})
        print(f"  {cuisine:<10}{c.get('count')}")

    _, sample = client.call("POST", f"/{INDEX}/_search", {"size": 1})
    hits = sample.get("hits", {}).get("hits", [])
    if hits:
        print("Example document:", json.dumps(hits[0]["_source"]))
    print("\nDone.")


if __name__ == "__main__":
    main()
