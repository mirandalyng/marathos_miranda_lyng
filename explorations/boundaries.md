# Data Boundaries — Silver Layer (marathos.silver.marathos_obt)

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
- Event distances with `h` in `HH:MM` format are converted to decimal hours
- Athlete performance times in `HH:MM:SS` format are converted to decimal hours