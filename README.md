# Dining Concierge Chatbot (Cloud Computing and Big Data, Fall 2026, Assignment 1)

**Author:** Adhyayan Verma, NYU Tandon

A serverless, microservice-driven chatbot that collects dining preferences through conversation and emails restaurant suggestions.

## Architecture

```
User -> S3 (frontend) -> API Gateway -> LF0 -> Amazon Lex V2 -> LF1 -> SQS (Q1)
                                                                    |
EventBridge Scheduler (every 1 min) -> LF2 -> OpenSearch (random IDs by cuisine)
                                          -> DynamoDB yelp-restaurants (details)
                                          -> SES (email to user)
                                          -> DynamoDB user-state (last search, extra credit)
```

All resources are in **us-east-1**.

## Repository layout

| Folder | Contents |
|---|---|
| `frontend/` | Starter chat UI hosted on S3, with the generated API Gateway JavaScript SDK. `chat.js` sends a session ID (`<userId>.<random>`) so Lex keeps conversation state; the user ID persists in the browser for the extra credit. |
| `lambda-functions/LF0` | API Lambda: takes the API request, sends the text to Lex (`RecognizeText`), returns Lex's reply in the Swagger `BotResponse` format. |
| `lambda-functions/LF1` | Lex code hook: handles GreetingIntent, ThankYouIntent, DiningSuggestionsIntent; validates slots; pushes requests to SQS Q1; extra-credit "same as last time" logic. |
| `lambda-functions/LF2` | Queue worker: pulls from Q1, gets random restaurant IDs for the cuisine from OpenSearch, fetches details from DynamoDB, emails them with SES, saves the user's last search. |
| `other-scripts/` | `load_restaurants.py` (builds the DynamoDB dataset), `load_opensearch.py` (builds the OpenSearch index), `swagger.yaml` (API spec), `lex-bot-setup.md` (bot configuration). |

## Requirements mapping

| # | Requirement | Implementation |
|---|---|---|
| 1 | Frontend on S3 | Static website hosting on S3, public read bucket policy |
| 2 | API | API Gateway REST API imported from `swagger.yaml`, `POST /chatbot` -> LF0 (non-proxy), CORS enabled |
| 3 | Lex bot | Lex V2 bot with 3 intents; LF1 validates location, cuisine, party size, date, time, email; sends to SQS Q1 |
| 4 | Lex integration | LF0 calls `lexv2-runtime.recognize_text` |
| 5 | Restaurant data | DynamoDB table `yelp-restaurants` (see Data sources) |
| 6 | OpenSearch | Index `restaurants`, documents `{RestaurantID, Cuisine, type: "Restaurant"}` |
| 7 | Suggestions module | LF2 triggered every minute by EventBridge Scheduler; OpenSearch + DynamoDB + SES |
| EC | Remember last search | DynamoDB table `user-state`; if location and cuisine match the last search, the bot asks whether to resend the same recommendations |

## Data sources (deviation from Yelp, approved by TA)

Instead of the Yelp API, restaurant data comes from:

1. **NYC DOHMH Restaurant Inspection Results** (NYC Open Data, dataset `43nn-pn8j`): Manhattan restaurants, de-duplicated by `CAMIS`. Provides Business ID, name, address, zip code, coordinates and cuisine.
2. **UCSD McAuley Lab Google Local (2021), New York metadata**: average rating and number of reviews, matched to DOHMH restaurants by normalized name and distance (within 150 m). Restaurants marked permanently closed are skipped.

Only restaurants with a matched rating are stored, so every item has all required fields: BusinessID, Name, Address, Coordinates, NumberOfReviews, Rating, ZipCode, plus `insertedAtTimestamp`. Cuisines: chinese, italian, japanese, indian, mexican, thai (1,183 restaurants total; the TA confirmed 200 per cuisine is not required). The table keeps the required name `yelp-restaurants`.

## Notes

- **OpenSearch mapping types:** OpenSearch 7+ removed mapping types, so the `Restaurant` type is stored as a field (`"type": "Restaurant"`) on each document in the `restaurants` index.
- **Lex version:** Lex V1 is deprecated, so the bot uses Lex V2 (event and response formats follow V2).
- **SES:** the account is in the SES sandbox, so sender and recipient addresses are verified identities.
- **Secrets:** no credentials are stored in code. Queue URLs, bot IDs and OpenSearch credentials are Lambda environment variables.

## Lambda environment variables

| Function | Variables |
|---|---|
| LF0 | `BOT_ID`, `BOT_ALIAS_ID`, `LOCALE_ID` (`en_US`) |
| LF1 | `QUEUE_URL`, `STATE_TABLE` (`user-state`) |
| LF2 | `QUEUE_URL`, `SENDER_EMAIL`, `TABLE_NAME` (`yelp-restaurants`), `STATE_TABLE`, `OPENSEARCH_ENDPOINT`, `OPENSEARCH_USER`, `OPENSEARCH_PASSWORD` |

All functions use Python 3.12.
