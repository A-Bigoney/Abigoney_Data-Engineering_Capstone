import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

immigration_table_drop =  "DROP TABLE IF EXISTS immigration;"
cities_demog_table_drop =  "DROP TABLE IF EXISTS cities_demog;"
airport_codes_table_drop =  "DROP TABLE IF EXISTS airport_codes;"
city_surface_temps_table_drop =  "DROP TABLE IF EXISTS city_surface_temps;"
state_surface_temps_table_drop =  "DROP TABLE IF EXISTS state_surface_temps;"


OLDimmigration_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration (
		_c0				int,
		cicid			int8,
		i94yr			int4,
		i94mon			int2,
		i94cit			int,
		i94res			int,
		i94port			varchar,
		arrdate			int,
		i94mode			int,
		i94addr			varchar,
		depdate			int,
		i94bir			int,
		i94visa			int,
		count			int,
		dtadfile		int,
		visapost		varchar,
		occup			varchar,
		entdepa			varchar(1),
		entdepd			varchar(1),
		entdepu			varchar,
		matflag			varchar(1),
		biryear			int,
		dtaddto			int,
		gender			varchar(1),
		insnum			varchar,
		airline			varchar,
		admnum			int8,
		fltno			varchar,
		visatype		varchar(2)
	);
""")

cities_demog_table_create = ("""
    CREATE TABLE IF NOT EXISTS cities_demog (
		city					varchar(256),
        state					varchar(256),
        median_age				float,
        male_population			int,
        female_population		int,
        total_population		int,
        veteran_population		int,
        foreign_born			int,
        ave_household_size		float,
        state_code				varchar(2),
        race					varchar(256),
		count					int
	);
""")

airport_codes_table_create = ("""
    CREATE TABLE IF NOT EXISTS airport_codes (
		ident			varchar(5),
		type			varchar(256),
		airtport_name	varchar(256),
		elevation_ft	int,
		continent		varchar(256),
		iso_country		varchar(2),
		iso_region		varchar(5),
		municipality	varchar(256),
		gps_code		varchar(4),
		iata_code		varchar(256),
		local_code		varchar(4),
		latitude		float,
        longitude		float                      
	);
""")

immigration_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration (
		cicid			int8,
		i94yr			int4,
		i94mon			int2,
		i94cit			int,
		i94res			int,
		i94port			varchar,
		arrdate			int,
		i94mode			int,
		i94addr			varchar,
		depdate			int,
		i94bir			int,
		i94visa			int,
		count			int,
		dtadfile		int,
		visapost		varchar,
		occup			varchar,
		entdepa			varchar(1),
		entdepd			varchar(1),
		entdepu			varchar,
		matflag			varchar(1),
		biryear			int,
		dtaddto			int,
		gender			varchar(1),
		insnum			varchar,
		airline			varchar,
		admnum			int8,
		fltno			varchar,
		visatype		varchar(2)
	);
""")

city_surface_temps_table_create = ("""
    CREATE TABLE IF NOT EXISTS city_surface_temps(
		year								int,
		mon									int,
		average_temperature					float   	NOT NULL,
		average_temperature_uncertainty		float,
		city								varchar(256),
		country								varchar(256),
		latitude							varchar(256),
		longitude							varchar(256)
	);
""")

state_surface_temps_table_create = ("""
    CREATE TABLE IF NOT EXISTS state_surface_temps(
		year								int,
		mon									int,
		average_temperature					float   	NOT NULL,
		average_temperature_uncertainty		float,
		state								varchar(256),
		country								varchar(256)
	);
""")

drop_table_queries = [airport_codes_table_drop, immigration_table_drop, cities_demog_table_drop, city_surface_temps_table_drop, state_surface_temps_table_drop]
create_table_queries = [immigration_table_create, cities_demog_table_create, airport_codes_table_create, city_surface_temps_table_create, state_surface_temps_table_create]
