CREATE TABLE tracks (
	id TEXT PRIMARY KEY,
	name TEXT,
	popularity INT,
	duration_ms INT,
	explicit BOOL,
	artists TEXT[],
	id_artists TEXT[],
	release_date DATE,
	danceability VARCHAR(9),
	energy float8,
	key SMALLINT,
	loudness float8,
	mode BOOLEAN,
	speechiness float8,
	acousticness float8,
	instrumentalness float8,
	liveness float8,
	valence float8,
	tempo float8,
	time_signature SMALLINT,
	year SMALLINT,
	month SMALLINT,
	day SMALLINT
);

CREATE TABLE artists (
	id TEXT PRIMARY KEY,
	followers INT,
	genres TEXT[],
	name TEXT,
	popularity SMALLINT
);

CREATE TEMP TABLE temp_json_import_tracks (data JSONB);

COPY temp_json_import_tracks FROM 'C:/Program Files/PostgreSQL/17/data/tracks.json';

INSERT INTO tracks (id,name,popularity,duration_ms,explicit,artists,id_artists,release_date,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature)
SELECT 
    data->>'id', 
    data->>'name', 
    (data->>'name')::INT, 
    (data->>'duration_ms')::INT,
	(data->>'explicit')::INT,
	ARRAY(SELECT jsonb_array_elements_text(data->'artists')),
	ARRAY(SELECT jsonb_array_elements_text(data->'id_artists')),
	(data->>'release_date')::DATE,
	data->>'danceability',
	(data->>'energy')::float8,
	(data->>'key')::SMALLINT,
	(data->>'loudness')::float8,
	(data->>'mode')::BOOLEAN,
	(data->>'speechiness')::float8,
	(data->>'acousticness')::float8,
	(data->>'instrumentalness')::float8,
	(data->>'liveness')::float8,
	(data->>'valence')::float8,
	(data->>'tempo')::float8,
	(data->>'time_signature')::SMALLINT,
	(data->>'year')::SMALLINT,
	(data->>'month')::SMALLINT,
	(data->>'day')::SMALLINT
FROM temp_json_import_tracks;



