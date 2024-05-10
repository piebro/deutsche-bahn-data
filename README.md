# Deutsche Bahn Data

A german version of this readme is [here](README_de.md).

This is a repository with accumulated public data from "Deutsche Bahn", the biggest german train company and a website with some generated interactive plots and tables using the data.

TODO: example plot image?

## The Data

The data is a list of entries when trains planed to arrive and departure in stations and their delay, platform change and if the station was canceled. The data is saved as a table in `data.csv` with these columns:

| Column Name              | Description                                                                                           |
|--------------------------|-------------------------------------------------------------------------------------------------------|
| **station**              | Represents the station name associated with the particular data entry.                               |
| **train_name**           | Combines the train type and line number, e.g. RE 1.                            |
| **final_destination_station** | The final destination station of the train.          |
| **arrival_planned_time** | Displays the scheduled arrival time at this station. Formatted as a date-time value. |
| **arrival_time_delta_in_min** | The difference, in minutes, between the planned arrival time and the actual or changed arrival time. Positive values indicate a delay, while negative values mean early arrival. |
| **departure_planned_time** | Shows the originally planned departure time at this station. Formatted as a date-time value.      |
| **departure_time_delta_in_min** | The difference, in minutes, between the planned departure time and the actual or changed departure time. Positive values show delays, while negative values reflect early departure. |
| **planned_platform**     | Represents the platform where the train was originally scheduled to arrive and/or depart.            |
| **changed_platform**     | Displays the platform that the train will now arrive and/or depart from, if it differs from the planned platform. |
| **stop_canceled**        | A boolean column (`True` or `False`) indicating whether this train stop at the station was canceled. |
| **train_type**           | Specifies the type of train, such as IC, ICE, EC, or other regional and local types.                  |
| **train_line_ride_id**        | Identifies the particular train line id this train ride is associated with.                                  |
| **train_line_station_num** | Represents the station's number in the sequence of stations on this particular train line.     |


### Data Collection

The [timetables-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables) is used to collect the raw data. It's free to query the api up to 60 times per seconde and the data is licensed as [(CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

The [timetable-plan-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/operation/%2Fplan%2F{evaNo}%2F{date}%2F{hour}/get) is used to get the planned timetable for a station at a specific hour and day. The [timetable-changes-api](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/operation/%2Ffchg%2F{evaNo}/get) is used to get all change. This API is queried every 6 hours to not miss any changes.

The responses of the APIs is saved in the data folder. Each day is a new subfolder and the suffix of each file hour in UTS time when the change request was made or the time of the planned train schedule.

You can look at the api using the website https://editor.swagger.io/ together with OpenAPI Documentation you can download from [here](https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/overview).

### How to get the biggest train stations with an eva number?

There are [german railway station categories](https://en.wikipedia.org/wiki/German_railway_station_categories) ([german link](https://de.wikipedia.org/wiki/Preisklasse)) we can use to get the biggest train stations. There is a table of each german train station with its category [here](https://www.deutschebahn.com/resource/blob/11895816/ef4ecf6dd8196c7db3ab45609d8a2034/Stationspreisliste-2024-data.pdf). This is used to get all train stations in catgory 1 and 2.

Next the eva number is needed for these train stations to use them in the API. There is an older list of train stations and their evas from 2014 [here](https://wiki.openstreetmap.org/w/images/c/c2/20141001_IBNR.pdf) and I could find a newer one. If you know of a new one, please write me or open an issue.

There is a script to automate the extraction and name matching called ``save_eva_name_list.py` to create the `eva_name_list.txt`. Run the script using the following commands.

```bash
# download the two pdfs with the data
wget https://www.deutschebahn.com/resource/blob/11895816/ef4ecf6dd8196c7db3ab45609d8a2034/Stationspreisliste-2024-data.pdf
wget https://wiki.openstreetmap.org/w/images/c/c2/20141001_IBNR.pdf

# install dependancies for the script
pip3 install tabula-py PyPDF2

# run the script
python3 save_eva_name_list.py
```
