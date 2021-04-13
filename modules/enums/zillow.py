# do not change the string values of these enums, they're specific to the csv import process
zillow_column_region_id = "RegionID"
zillow_column_size_rank = "SizeRank"
zillow_column_region_name = "RegionName"
zillow_column_region_type = "RegionType"
zillow_column_state_name = "StateName"
zillow_column_state = "State"
zillow_column_city = "City"
zillow_column_metro = "Metro"
zillow_column_county_name = "CountyName"

# these string values are for the formatted output
custom_column_housing_type = "Housing Type"
custom_column_date = "Date"
custom_column_zhvi = "ZHVI"
custom_column_appreciation = '% Per Year'
custom_column_zhvi_start = 'Starting ZHVI'
custom_column_zhvi_end = 'Final ZHVI'
custom_column_years_difference = 'Years Available'
custom_column_start_date = 'Start Date'
custom_column_end_date = 'End Date'
custom_column_zhvi_min = 'ZHVI Min'
custom_column_zhvi_max = 'ZHVI Max'
custom_column_percent_min = '% Min'
custom_column_percent_max = '% Max'
custom_column_percent_avg = '% Avg'
custom_column_percent_stddev = '% Stddev'
custom_column_years_min = 'Years Min'
custom_column_years_max = 'Years Max'
custom_column_years_avg = 'Years Avg'
custom_column_years_stddev = 'Years Stddev'

zillow_named_column_indexes = [zillow_column_region_id, zillow_column_size_rank, zillow_column_region_name,
                               zillow_column_region_type, zillow_column_state_name, zillow_column_state,
                               zillow_column_city, zillow_column_metro, zillow_column_county_name]

zillow_data_type_condo = "Condo"
zillow_data_type_sfr = "SingleFamilyResidence"
zillow_data_housing_types = [zillow_data_type_condo, zillow_data_type_sfr]
