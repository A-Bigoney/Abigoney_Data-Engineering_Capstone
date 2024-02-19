fact_table:
    average_temperature                DoubleType:  The average temperature in a specific city and year.
    average_temperature_uncertainty    DoubleType:  The uncertainty associated with the average temperature measurement.
    city                               StringType:  The name of the city.
    country                            StringType:  The name of the country.
    latitude                           DoubleType:  The latitude coordinate of the city.
    longitude                          DoubleType:  The longitude coordinate of the city.
    year                               IntegerType: The year of the temperature measurement.
    mon                                IntegerType: The month of the temperature measurement.
    ident                              StringType:  The identifier of the airport.
    type                               StringType:  The type of the airport.
    airport_name                       StringType:  The name of the airport.
    elevation_ft                       IntegerType: The elevation of the airport in feet.
    continent                          StringType:  The continent where the airport is located.
    iso_country                        StringType:  The ISO code of the country.
    iso_region                         StringType:  The ISO code of the region.
    municipality                       StringType:  The municipality where the airport is located.
    gps_code                           StringType:  The GPS code of the airport.
    iata_code                          StringType:  The IATA code of the airport.
    local_code                         StringType:  The local code of the airport.
    ave_household_size                 DoubleType:  The average household size in the city.
    count                              IntegerType: The count of records.
    female_population                  IntegerType: The number of female individuals in the city.
    foreign_born                       IntegerType: The number of foreign-born individuals in the city.
    median_age                         DoubleType:  The median age of the population in the city.
    veteran_population                 IntegerType: The number of veterans in the city
    race                               StringType:  The race or ethnicity category.
    state                              StringType:  The name of the state.
    state_code                         StringType:  The code of the state.
    total_population                   DoubleType:  The total population in the city.

dimension_table_demographics:
    city                StringType:  The name of the city.
    state               StringType:  The name of the state.
    median_age          DoubleType:  The median age of the population in the city.
    male_population     IntegerType: The number of male individuals in the city.
    female_population   IntegerType: The number of female individuals in the city.
    total_population    DoubleType:  The total population in the city.
    veteran_population  IntegerType: The number of veterans in the city.
    foreign_born        IntegerType: The number of foreign-born individuals in the city.
    ave_household_size  DoubleType:  The average household size in the city.
    state_code          StringType:  The code of the state.
    race                StringType:  The race or ethnicity category.
    count               IntegerType: The count of individuals in each race or ethnicity category.

dimension_table_airport:
    municipality        StringType: The municipality where the airport is located.
    elevation_ft        IntegerType: The elevation of the airport in feet.
    airport_name        StringType: The name of the airport.
    iso_country         StringType: The ISO code of the country.
    iso_region          StringType: The ISO code of the region.

Please note that the data types mentioned above (e.g., StringType, DoubleType, IntegerType) correspond to the data types used in Apache Spark's DataFrame API.