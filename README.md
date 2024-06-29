# Deutsche Bahn Data

This is a repository with accumulated public data from "Deutsche Bahn", the biggest german train company. The data is fetched and saved from the public DB API 4 times a day using github actions. The data is used to create a [website](https://piebro.github.io/deutsche-bahn-statistics) with statistics about train delays and canceled trains.

## Data Collection

The [timetables-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables) is used to collect the raw data. It's free to query the api up to 60 times per seconde and the data is licensed as [(CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

The [timetable-plan-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/operation/%2Fplan%2F{evaNo}%2F{date}%2F{hour}/get) is used to get the planned timetable for a station at a specific hour and day. The [timetable-changes-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/operation/%2Ffchg%2F{evaNo}/get) is used to get all change. This API is queried every 6 hours to not miss any changes.

The API is queried using the evas from the biggest train stations. The responses are saved in the `data` folder. Each day is a new subfolder and the suffix of each file hour in UTS time when the change request was made or the time of the planned train schedule.

You can look at the api using the website https://editor.swagger.io/ together with OpenAPI Documentation you can download from [here](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/overview).

An example curl command to query the plan api:
```bash
curl -s -H "DB-Api-Key: $API_KEY" -H "DB-Client-Id: $CLIENT_ID" -H "accept: application/xml" "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/08000260/$(date +"%y%m%d")/$(date +"%H")"
```

## How to get the biggest train stations and their EVA number?

There are [german railway station categories](https://en.wikipedia.org/wiki/German_railway_station_categories) ([german link](https://de.wikipedia.org/wiki/Preisklasse)) we can use to get the biggest train stations. There is a table of each german train station with its category [here](https://www.dbinfrago.com/web/bahnhoefe/leistungen/stationsnutzung/stationshalt/Stationspreise-10995752). This is used to get all train stations in catgory 1 and 2.

Using the [timetable-station-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/operation/%2Fstation%2F{pattern}/get), the evas from station names from the list above can be retrieved.

There is a script called `get_eva_list.py` to save the EVAs of all relevant train stations. It doesn't work for all station names, the remaining ones can be looked up here: https://wiki.openstreetmap.org/w/images/c/c2/20141001_IBNR.pdf. For running the script you need to install `pdfplumber`.

Some train stations also have multiple evas (out of historical or other reasons). There is a script to get them, together with the name: `get_additional_evas_of_trainstations.py`.

## Contributing

Contriutions are welcome. Open an Issue if you want to report a bug, have an idea or want to propose a change.

## License

All code in this project is licensed under the MIT License. The [data](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables) is licensed under [Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) by Deutsche Bahn.