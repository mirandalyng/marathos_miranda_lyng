# Data Decisions — Silver Layer (marathos.silver.marathos_obt)

## Athlete Age
- Athlete must be **at least 18 years old** at the time of the event, calculated as `year_of_event - athlete_year_of_birth >= 18`
- Athlete must be **no older than 83 years** at the time of the event, calculated as `year_of_event - athlete_year_of_birth <= 83`, based on the oldest verified ultra marathon finisher being 83 years old
- Athlete year of birth must be **1700 or later** to filter out clearly invalid birth years

## Event Distance
- Events with **multi-stage or multi-day formats** are excluded, identified by keywords such as `Etappen`, `tage`, `days` in the `event_distance/length` column
- Events with **slash-separated distances** (e.g. `105/2Etappen`) are excluded as they represent multi-stage formats that cannot be accurately converted to a single distance
- Events with **comma-separated distances** (e.g. `12,8,4`) are excluded as they represent invalid or unparseable formats

## Required Fields
The following columns must not be null for a record to be included:
- `athlete_id` — required to identify the athlete
- `year_of_event` — required for age validation and event context
- `event_dates` — required for accurate temporal filtering and age calculation

## Performance Validity
- Only performances where the **unit matches the event type** are kept:
  - For distance events (`km`, `mi`): athlete performance must be in time (`h`)
  - For timed events (`h`): athlete performance must be in distance (`km`)
- Performances containing `d` (days) are excluded as invalid

## Units & Conversions
- Distances in miles (`mi`) are converted to kilometers using the factor `1.60934`
- Event distances in `HH:MM` format are converted to decimal hours
- Athlete performance times in `HH:MM:SS` format are converted to decimal hours

## Identifiers
All identifiers are generated using **SHA-256 hashing** (`sha2(..., 256)`) to produce stable, collision-resistant keys that are safe for use in streaming tables.

| Column | Input | Scope |
|---|---|---|
| `event_id` | `event_name` | Uniquely identifies an event regardless of date — multiple races of the same event share this ID |
| `race_id` | `event_name` + `event_dates` | Uniquely identifies a single occurrence (race) of an event on a specific date |
| `result_id` | `race_id` + `athlete_id` | Uniquely identifies one athlete's result in a specific race |

> **Note:** `event_id` is broader than `race_id` — the same event held annually will have one `event_id` but a distinct `race_id` per year.

## Data Cleaning
- **Event names** are stripped of leading `#`, `"`, and `*` characters; names that are empty after cleaning are replaced with `unknown`
- **Athlete club** values are stripped of leading `#`, `"`, and `*` characters; empty values are replaced with `unknown`
- **Athlete country** empty values are replaced with `unknown`
- **Athlete gender** and **age category** empty values are replaced with `unknown`
- **Event country** is extracted from the event name using the three-letter country code in parentheses, e.g. `Berlin Marathon (GER)` → `GER`, and resolved to a full country name via a country reference table matching on ISO Alpha-3 code and long name 
  - I chose to use a real dataset to get more value out of the assignment instead of using LMM
- **Athlete country** is resolved to a full country name via the same country reference table, matching on both ISO Alpha-3 code and long name