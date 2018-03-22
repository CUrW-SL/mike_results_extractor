import csv
from datetime import datetime

from config import COMMON_TIMESTAMP_FORMAT


def _format_timestamp(date_time, input_format, output_format):
    date_time_in = datetime.strptime(date_time, input_format)
    date_time_out = datetime.strftime(date_time_in, output_format)
    return date_time_out


def read_csv_file(file_location, input_timestamp_format=COMMON_TIMESTAMP_FORMAT,
                  output_timestamp_format=COMMON_TIMESTAMP_FORMAT, station_list=None):
    """
    Function to extract the data from a given .csv file.
    Here the .csv file contains timeseries values for A particular variable(either Water-Level,
    or Water-Discharge, or some-other).
    The .csv file is consisted of values for multiple stations.
    First line is metadata. [Timestamp(default"%m/%d/%Y %H:%M"),<station1-name>,<station1-name>,<station2-name>,...]

    :param file_location:
    :param input_timestamp_format:
    :param output_timestamp_format:
    :param station_list:
    :return: matrix_meta_data, data_matrix
    """

    with open(file_location) as csvfile:
        # Read the scv file. will get an array of arrays.
        read_csv = csv.reader(csvfile, delimiter=',')
        # Destructuring the read_csv array to separate meta-data and data.
        meta_data, *data_matrix = read_csv

        if not isinstance(meta_data, list) or not isinstance(data_matrix, list):
            print("Invalid csv file; \nmeta_data: %s" % meta_data)
            return None

        extract_index_list = []  # to collect indices that should be extracted, when station_list is given.
        output_meta_data = []    # to collect meta_data corresponding to the given station_list.
        if station_list is not None:
            output_meta_data.append(meta_data[0])
            for station in station_list:
                if station in meta_data:
                    extract_index_list.append(meta_data.index(station))
                    output_meta_data.append(station)

        if station_list is not None:
            # If the station_list is given, then extract_index_list and corresponding output_meta_data
            # should not be empty.
            if len(extract_index_list) > 0 and len(output_meta_data) > 0:
                print("Extracting data of the given station list. Meta data list: ", output_meta_data)

                output_data_matrix = []
                for row in data_matrix:

                    # Format the timestamp (row[0]) to comply with :param output_timestamp_format.
                    data_row = [_format_timestamp(row[0], input_timestamp_format, output_timestamp_format)]
                    for item_index in extract_index_list:
                        data_row.append(row[item_index])

                    output_data_matrix.append(data_row)

                return output_meta_data, output_data_matrix
            else:
                print("No matching station can be found corresponding to the given station_list. \n" +
                      "Given station-List: %s \n Meta data of csv: %s \n" % (station_list, meta_data))
                return None

        else:
            print("Extracting data of all the available stations. Meta data list: ", meta_data)

            # Iterate all the rows of the data_matrix and format the timestamp (row[0]) to comply with
            # :param output_timestamp_format.
            for row in data_matrix:
                row[0] = _format_timestamp(row[0], input_timestamp_format, output_timestamp_format)

            return meta_data, data_matrix


def extract_single_timeseries(matrix_meta_data, data_matrix, station_name):
    """
    From matrix_meta_data and data_matrix, get the timeseries of given station_name.
    :param matrix_meta_data:
    :param data_matrix:
    :param station_name:
    :return:
    """
    station_index = matrix_meta_data.index(station_name) if station_name in matrix_meta_data else None

    if station_index is not None:
        timeseries = []
        for row in data_matrix:
            timeseries.append([row[0], row[station_index]])
        return timeseries
    else:
        return []


def get_timeseries_in_between(timeseries, start_date, end_date):
    """
    Filter and get timeseries in between given start (inclusive) and end (exclusive) dates
    :param start_date:
    :param end_date:
    :return: timeseries
    """
    return [elem for elem in timeseries
            if start_date <= datetime.strptime(elem[0], COMMON_TIMESTAMP_FORMAT) < end_date]



