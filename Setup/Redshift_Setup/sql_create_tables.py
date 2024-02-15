import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

immigration_table_drop =  "DROP TABLE IF EXISTS immigration;"
cities_demog_table_drop =  "DROP TABLE IF EXISTS cities_demog;"
airport_codes_table_drop =  "DROP TABLE IF EXISTS airport_codes;"


staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
time_table_drop = "DROP TABLE IF EXISTS time;"

immigration_table_create = ("""
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
		name			varchar(256),
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

create_songplays_table = ("""
	CREATE TABLE public.songplays (
		playid varchar(32) NOT NULL,
		start_time timestamp NOT NULL,
		userid int4 NOT NULL,
		"level" varchar(256),
		songid varchar(256),
		artistid varchar(256),
		sessionid int4,
		location varchar(256),
		user_agent varchar(256),
		CONSTRAINT songplays_pkey PRIMARY KEY (playid)
	);
""")

create_songs_table = ("""
	CREATE TABLE public.songs (
		songid varchar(256) NOT NULL,
		title varchar(256),
		artistid varchar(256),
		"year" int4,
		duration numeric(18,0),
		CONSTRAINT songs_pkey PRIMARY KEY (songid)
	);
""")

create_staging_events_table = ("""
	CREATE TABLE public.staging_events (
		artist varchar(256),
		auth varchar(256),
		firstname varchar(256),
		gender varchar(256),
		iteminsession int4,
		lastname varchar(256),
		length numeric(18,0),
		"level" varchar(256),
		location varchar(256),
		"method" varchar(256),
		page varchar(256),
		registration numeric(18,0),
		sessionId int4,
		song varchar(256),
		status int4,
		ts int8,
		useragent varchar(256),
		userid int4
	);
""")

create_staging_songs_table = ("""
	CREATE TABLE public.staging_songs (
		num_songs int4,
		artist_id varchar(256),
		artist_name varchar(256),
		artist_latitude numeric(18,0),
		artist_longitude numeric(18,0),
		artist_location varchar(256),
		song_id varchar(256),
		title varchar(256),
		duration numeric(18,0),
		"year" int4
	);
""")

create_time_table = ("""
	CREATE TABLE public."time" (
		start_time timestamp NOT NULL,
		"hour" int4,
		"day" int4,
		week int4,
		"month" varchar(256),
		"year" int4,
		weekday varchar(256),
		CONSTRAINT time_pkey PRIMARY KEY (start_time)
	) ;
""")

create_users_table = ("""
	CREATE TABLE public.users (
		userid int4 NOT NULL,
		first_name varchar(256),
		last_name varchar(256),
		gender varchar(256),
		"level" varchar(256),
		CONSTRAINT users_pkey PRIMARY KEY (userid)
	);
""")


drop_table_queries = [airport_codes_table_drop, immigration_table_drop, cities_demog_table_drop]
create_table_queries = [immigration_table_create, cities_demog_table_create, airport_codes_table_create]
