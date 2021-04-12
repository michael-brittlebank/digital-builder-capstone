from ..files import *
from ._helpers import *
from ..enums import *


def get_baseline_data(is_only_amfam_data, is_raw_data):
    """
    get_baseline_data translates csv files into analysable files
    :param is_only_amfam_data: boolean for whether to return data in amfam's operating states or all states
    :param is_raw_data: boolean for whether to return formatted data or not
    :return: list of baseline data
    """
    default_groupby = [zillow_column_region_name, custom_column_housing_type]

    # raw_zillow_data = mysql_zillow.get_zillow_data() # db call

    raw_condo_data = import_csv("ingested-condo.csv", [file_path, file_export_path, file_export_path_testing])
    # raw_sfr_data = import_csv("ingested-singlefamilyresidence.csv", file_export_path_testing)

    # convert data to dataframes and merge together
    # headers = [zillow_column_region_name, custom_column_housing_type, zillow_column_state, zillow_column_city,
    #            zillow_column_metro, zillow_column_county_name, custom_column_date, custom_column_zhvi]
    conda_data = get_dataframe_from_list(raw_condo_data)
    # sfr_data = get_dataframe_from_list(raw_sfr_data)
    base_dataframe = pd.concat([conda_data])

    # massage data in dataframes
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].astype(float)  # cast string to float
    base_dataframe[custom_column_date] = pd.to_datetime(base_dataframe[custom_column_date])  # convert to pandas date

    # # convert data to dataframes

    # massage data in dataframes
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].replace([0],
                                                                                    np.nan)  # replace zero ZHVI with NAN to exclude from calculations
    base_dataframe.sort_values(
        by=[custom_column_date])  # sorting the data allows for percent change to be calculated correctly

    # calculate first valid zhvi value per row
    first_valid_dataframe = base_dataframe.pivot(index=default_groupby, columns=custom_column_date,
                                                 values=custom_column_zhvi)
    first_valid_zhvi = first_valid_dataframe.apply(pd.Series.first_valid_index, axis=1)

    # create summary dataframe
    summary_dataframe = pd.DataFrame()
    summary_dataframe[zillow_column_city] = base_dataframe.groupby(default_groupby)[zillow_column_city].first()
    summary_dataframe[zillow_column_state] = base_dataframe.groupby(default_groupby)[zillow_column_state].first()

    if is_only_amfam_data:
        summary_dataframe = filter_dataframe_by_amfam_states(summary_dataframe)

    summary_dataframe[custom_column_zhvi_start] = base_dataframe.groupby(default_groupby)[
        custom_column_zhvi].first()
    summary_dataframe[custom_column_start_date] = first_valid_zhvi
    summary_dataframe[custom_column_zhvi_end] = base_dataframe.groupby(default_groupby)[
        custom_column_zhvi].last()
    summary_dataframe[custom_column_end_date] = base_dataframe.groupby(default_groupby)[custom_column_date].last()
    summary_dataframe[custom_column_zhvi_min] = base_dataframe.groupby(default_groupby)[custom_column_zhvi].min()
    summary_dataframe[custom_column_zhvi_max] = base_dataframe.groupby(default_groupby)[custom_column_zhvi].max()
    summary_dataframe[custom_column_years_difference] = round(
        (summary_dataframe[custom_column_end_date] - summary_dataframe[custom_column_start_date]).dt.days / 365,
        1)  # estimate years from number of days between start and end
    summary_dataframe[custom_column_appreciation] = summary_dataframe.apply(
        lambda row: get_home_appreciation_percentage_per_year(row[custom_column_zhvi_start],
                                                              row[custom_column_zhvi_end],
                                                              row[custom_column_years_difference]), axis=1)
    summary_dataframe = summary_dataframe.sort_values(custom_column_appreciation,
                                                      ascending=False)  # sort table to find highest movers
    # format percentages
    if not is_raw_data:
        summary_dataframe[custom_column_appreciation] = summary_dataframe[custom_column_appreciation].apply(
            percent_formatter)
        summary_dataframe[custom_column_zhvi_start] = summary_dataframe[custom_column_zhvi_start].apply(
            currency_formatter)
        summary_dataframe[custom_column_zhvi_end] = summary_dataframe[custom_column_zhvi_end].apply(currency_formatter)
        summary_dataframe[custom_column_zhvi_min] = summary_dataframe[custom_column_zhvi_min].apply(currency_formatter)
        summary_dataframe[custom_column_zhvi_max] = summary_dataframe[custom_column_zhvi_max].apply(currency_formatter)
    return summary_dataframe


def calculate_baseline_data(data):
    header_mappings = {column_date: custom_column_date, column_zhvi: custom_column_zhvi}

    # create dataframe
    base_dataframe = pd.DataFrame(data).rename(columns=header_mappings)

    # sorting the data allows for percent change to be calculated correctly
    base_dataframe = base_dataframe.sort_values(custom_column_date, ascending=True)

    # replace zero ZHVI with NAN to exclude from calculations
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].replace([0], np.nan)

    # convert to pandas date
    base_dataframe[custom_column_date] = pd.to_datetime(base_dataframe[custom_column_date])

    first_valid_zhvi_index = base_dataframe[custom_column_zhvi].first_valid_index()

    # create summary dataframe
    summary_dataframe = {
        custom_column_zhvi_start: base_dataframe[custom_column_zhvi].iat[first_valid_zhvi_index],
        custom_column_start_date: base_dataframe[custom_column_date].iat[first_valid_zhvi_index],
        custom_column_zhvi_end: base_dataframe[custom_column_zhvi].iat[-1],
        custom_column_end_date: base_dataframe[custom_column_date].max(),
        custom_column_zhvi_min: base_dataframe[custom_column_zhvi].min(),
        custom_column_zhvi_max: base_dataframe[custom_column_zhvi].max()
    }

    summary_dataframe[custom_column_years_difference] = round(
        (summary_dataframe[custom_column_end_date] - summary_dataframe[custom_column_start_date]).days / 365,
        1)  # estimate years from number of days between start and end
    summary_dataframe[custom_column_appreciation] = get_home_appreciation_percentage_per_year(
        summary_dataframe[custom_column_zhvi_start],
        summary_dataframe[custom_column_zhvi_end],
        summary_dataframe[custom_column_years_difference])
    return summary_dataframe
