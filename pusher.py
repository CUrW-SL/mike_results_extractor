import argparse
import os
import copy
from datetime import datetime, timedelta

from curwmysqladapter import MySQLAdapter, Station, Data

from config import DB_CONFIGS, STATION_CONFIGS, get_station_info, EVENT_ID_META_DATA, COMMON_DATE_FORMAT
from extractor import read_csv_file, extract_single_timeseries, get_timeseries_in_between


USAGE = \
    """
    '-d' or '--data' help='.csv data file (should reside in resources folder) from which data should be extracted.'
    -f' or '--force' help='(optional)If true will overwrite if there is an already existing timeseries.'
    '-wl' or '--waterlevel' help='This specifies the WaterLevel variable type.'
    '-dc', '--discharge' help='This specifies the Discharge variable type.'
    Either -wl or -dc must be specified, not both, not none.
    """

timeseries_meta_struct = {
    'station': '',
    'variable': '',
    'unit': '',
    'type': '',
    'source': '',
    'name': ''
}


try:
    # Parse the commandline arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data',
                        help='.csv data file (should reside in resources folder) from which data should be extracted.')
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='If true will overwrite if there is an already existing timeseries.')
    parser.add_argument('-wl', '--waterlevel',
                        action='store_true',
                        help='This specifies the WaterLevel variable type.')
    parser.add_argument('-dc', '--discharge',
                        action='store_true',
                        help='This specifies the Discharge variable type.')
    args = parser.parse_args()
    print('\n\nCommandline arguments:', args)

    file_name = None
    force_insert = args.force if args.force else False
    is_water_level = args.waterlevel if args.waterlevel else False
    is_discharge = args.discharge if args.discharge else False

    # .csv data file name is mandatory.
    if args.data:
        file_name = args.data
    else:
        print("No data file is given.\nUsage: %s" % USAGE)
        exit(1)
    # Either -wl or -dc must be specified, not both, not none.
    if bool(is_water_level) == bool(is_discharge):
        print("Either -wl or -dc must be specified, not both, not none.\nUsage: %s" % USAGE)
        exit(1)

    # Retrieve station configs.
    stations = STATION_CONFIGS['stations']

    # Get the absolute path of the given .csv file.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    mike_wl_file = current_dir + '/resources/' + file_name

    # Retrieve data of all the stations from .csv file.
    meta_data, data_matrix = read_csv_file(mike_wl_file)
    if meta_data is None or data_matrix is None:
        print("Could not read any data from given .csv file.")
        exit(1)

    # Create database connections.
    mysql_adapter = MySQLAdapter(host=DB_CONFIGS['MYSQL_HOST'], user=DB_CONFIGS['MYSQL_USER'],
                                 password=DB_CONFIGS['MYSQL_PASSWORD'], db=DB_CONFIGS['MYSQL_DB'])

    # Loop through the stations and insert
    index = 0
    for meta_field in meta_data:
        # Ignore the timestamp column.
        if index == 0:
            index += 1
            continue

        # Ignore if the station info can be found in the STATION_CONFIGS.
        station_info = get_station_info(meta_field)
        if station_info is None:
            print("No station info can be found in STATION_CONFIGS corresponding to the station name: '%s' in csv file"
                  % meta_field)
            index += 1
            continue

        # IF station does not exist in the database create station.
        station_details_in_db = mysql_adapter.get_station({
            'stationId': station_info['station_Id'], 'name': station_info['station_name']})
        if station_details_in_db is None:
            print("Station: {stationId: %s, name: %s} does not exist in the DB. Creating station in the DB..."
                  % (station_info['station_Id'], station_info['station_name']))
            station_meta_list = [Station.MIKE11, station_info['station_Id'], station_info['station_name'],
                                 station_info['latitude'], station_info['longitude'],
                                 station_info['resolution'], station_info['description']]
            row_count = mysql_adapter.create_station(station_meta_list)
            if row_count > 0:
                print("Created new station: ", station_meta_list)
            else:
                print("Unable to create new station: ", station_meta_list)
                index += 1
                continue

        complete_timeseries = extract_single_timeseries(meta_data, data_matrix, meta_field)

        # Get start-date and end-date, divide and push timeseries accordingly wrt to EVENT_ID_META_DATA['types']
        start_date = datetime.strptime(EVENT_ID_META_DATA['start_date'], COMMON_DATE_FORMAT)
        end_date = datetime.strptime(EVENT_ID_META_DATA['end_date'], COMMON_DATE_FORMAT)
        num_dates = end_date - start_date

        if num_dates == timedelta(days=len(EVENT_ID_META_DATA['types'])):
            for i, event_type in enumerate(EVENT_ID_META_DATA['types']):
                print("EVENT TYPE: %s" % event_type)
                # Create event metadata. Event metadata is used to create timeseries id (event_id) for the timeseries.
                timeseries_meta = copy.deepcopy(timeseries_meta_struct)
                timeseries_meta['station'] = station_info['station_name']
                timeseries_meta['variable'] = 'Waterlevel' if is_water_level else 'Discharge'
                timeseries_meta['unit'] = 'm' if is_water_level else 'm3/s'
                timeseries_meta['type'] = event_type
                timeseries_meta['source'] = EVENT_ID_META_DATA['source']
                timeseries_meta['name'] = EVENT_ID_META_DATA['name']

                # At this point station exists. Check whether there is a timeseries_id (or event-id).
                # If does not exist create an id.
                timeseries_id = mysql_adapter.get_event_id(timeseries_meta)
                if timeseries_id is None:
                    print("No timeseries for the '%s' of the station: %s in the DB. Creating timeseries-id..."
                          % (timeseries_meta['variable'],
                             station_info['station_Id'] + '/' + station_info['station_name']))
                    timeseries_id = mysql_adapter.create_event_id(timeseries_meta)

                # Get corresponding timeseries.
                start = start_date + timedelta(days=i)
                end = start + timedelta(days=1)
                timeseries = get_timeseries_in_between(complete_timeseries, start, end)

                # Insert timeseries.
                print("Pushing to timeseries: %s of size %d" % (timeseries_id, len(timeseries)))
                inserted_rows = mysql_adapter.insert_timeseries(timeseries_id, timeseries, True, Data.data)
                print("Inserted %d rows from %d timeseries_id values successfully..." % (inserted_rows, len(timeseries)))
        else:
            print("start_date and end_date are not compatible with EVENT_ID_META_DATA types.\n"
                  "length of EVENT_ID_META_DATA types is not equal to date gap between start_date and end_date.")

        index += 1

except Exception as ex:
    print("An exception occurred while pushing data into the DB.", ex)
