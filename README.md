# Deutsche Bahn Data

This is a repository with accumulated public data from "Deutsche Bahn", the biggest german train company. The data is fetched and saved from the public DB API 4 times a day using github actions. The data is used to create a [website](https://piebro.github.io/deutsche-bahn-statistics/questions) with statistics about train delays and canceled trains. Similar website with statisics can be found at [github.com/piebro/deutsche-bahn-statistics](https://github.com/piebro/deutsche-bahn-statistics?tab=readme-ov-file#related-deutsche-bahn-and-open-data-websites)

## Data Collection

The [timetables-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables) is used to collect the raw data. It's free to query the api up to 60 times per seconde and the data is licensed as [(CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

The [timetable-plan-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/operation/%2Fplan%2F{evaNo}%2F{date}%2F{hour}/get) is used to get the planned timetable for a station at a specific hour and day. The [timetable-changes-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/operation/%2Ffchg%2F{evaNo}/get) is used to get all change. This API is queried every 6 hours to not miss any changes.

The API is queried using the evas from the biggest train stations To get the evas, the [Station Data API](https://developers.deutschebahn.com/db-api-marketplace/apis/product/stada) is used to get all station with [category](https://en.wikipedia.org/wiki/German_railway_station_categories) 1 or 2. The responses are saved in the `data` folder. Each day is a new subfolder and the suffix of each file hour in UTS time when the change request was made or the time of the planned train schedule.

You can look at the api using the website https://editor.swagger.io/ together with OpenAPI Documentation you can download from [here](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/overview).

An example curl command to query the plan api:
```bash
curl -s -H "DB-Api-Key: $API_KEY" -H "DB-Client-Id: $CLIENT_ID" -H "accept: application/xml" "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/08000260/$(date +"%y%m%d")/$(date +"%H")"
```

## Database Column Descriptions

The database contains the following columns that track train schedules, delays, and changes:

### Core Information
- `station`: String - The name of the station where the train stop occurs
- `train_name`: String - The identifier of the train, combining train type and number (e.g., "IC 123" or "RE 10")
- `train_type`: String - The type of train service (e.g., "IC", "ICE", "EC", "RE")
- `final_destination_station`: String - The final destination station of the train's journey

### Timing Information
- `delay_in_min`: Integer - The delay in minutes (calculated from departure delay if available, otherwise arrival delay)
- `time`: Timestamp - The effective time of the train stop (uses departure time if available, otherwise arrival time)
- `arrival_planned_time`: Timestamp - The scheduled arrival time at the station
- `arrival_change_time`: Timestamp - The actual/modified arrival time. If no changes occurred, equals the planned time
- `departure_planned_time`: Timestamp - The scheduled departure time from the station
- `departure_change_time`: Timestamp - The actual/modified departure time. If no changes occurred, equals the planned time

### Status Information
- `is_canceled`: Boolean - Indicates whether the train stop was canceled (true) or not (false)

### Train Line Information
- `train_line_ride_id`: String - Unique identifier for a specific train journey
- `train_line_station_num`: Integer - The sequential number of this station stop within the train's journey


## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/piebro/deutsche-bahn-data.git
   cd deutsche-bahn-data
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a monthly data release:
   ```bash
   python create_monthly_data_release.py "YYYY-MM"
   ```
   Replace `YYYY-MM` with the desired year and month.

## Contributing

Contributions are welcome. Open an Issue if you want to report a bug, have an idea or want to propose a change.

## License

All code in this project is licensed under the MIT License. The [data](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables) is licensed under [Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) by Deutsche Bahn.
