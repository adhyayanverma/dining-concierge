# Lex V2 bot configuration (DiningConcierge)

Language: English (US). Lambda code hook (LF1) attached on TestBotAlias, English (US).

## Slot types
- CuisineType (Expand values): chinese, italian, japanese, indian, mexican, thai
- LocationType (Expand values): manhattan, new york, nyc, new york city

## Intents
- GreetingIntent: "Hi", "Hello", "Hey", "Hi there", "Good morning", "Good evening". Fulfillment: LF1.
- ThankYouIntent: "Thank you", "Thanks", "Thanks a lot", "Thank you so much", "Appreciate it". Fulfillment: LF1.
- DiningSuggestionsIntent: initialization/validation hook and fulfillment hook both use LF1.

| Slot | Type | Required |
|---|---|---|
| Location | LocationType | yes |
| Cuisine | CuisineType | yes |
| NumberOfPeople | AMAZON.Number | yes |
| DiningDate | AMAZON.Date | yes |
| DiningTime | AMAZON.Time | yes |
| Email | AMAZON.EmailAddress | yes |
| SameAsLast | AMAZON.Confirmation | no (asked by LF1 only for repeat searches, extra credit) |
