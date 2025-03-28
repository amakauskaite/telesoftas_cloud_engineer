-- Create tables
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

-- Attempt to import data from json files
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

-- Fill tables with temporary test data


INSERT INTO music.tracks (
    id, name, popularity, duration_ms, explicit, artists, id_artists, release_date, danceability, energy, key, loudness, mode, 
    speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature, year, month, day
) VALUES 
(
    'track1', 'Song One', 85, 210000, FALSE, ARRAY['Artist A', 'Artist B'], ARRAY['idA', 'idB'], '2023-06-15', 'High', 0.8, 5, -6.2, TRUE,
    0.05, 0.1, 0.0, 0.12, 0.75, 120.5, 4, 2023, 6, 15
),
(
    'track2', 'Song Two', 70, 180000, TRUE, ARRAY['Name "Nickname" Surname'], ARRAY['idC'], '2018-03-21', 'Medium', 0.6, 3, -5.0, FALSE,
    0.08, 0.2, 0.0, 0.15, 0.6, 98.7, 3, 2018, 3, 21
),
(
    'track3', 'Song Three', 60, 240000, FALSE, ARRAY['Boy''s Choir'], ARRAY['idD'], '2015-11-10', 'Low', 0.4, 7, -8.3, TRUE,
    0.02, 0.5, 0.1, 0.18, 0.55, 105.2, 4, 2015, 11, 10
),
(
    'track4', 'Song Four', 90, 200000, TRUE, ARRAY['artist 1', 'artist''s brother', 'my "special" artist'], ARRAY['idE', 'idF', 'idG'], '2020-07-05', 'High', 0.85, 8, -4.5, FALSE,
    0.03, 0.12, 0.05, 0.20, 0.8, 130.1, 4, 2020, 7, 5
),
(
    'track5', 'Song Five', 50, 150000, FALSE, ARRAY['Solo Artist'], ARRAY['idH'], '2010-01-25', 'Low', 0.3, 2, -10.1, TRUE,
    0.06, 0.25, 0.0, 0.10, 0.4, 88.4, 3, 2010, 1, 25
);

INSERT INTO music.tracks (
    id, name, popularity, duration_ms, explicit, artists, id_artists, release_date, danceability, energy, key, loudness, mode, 
    speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature, year, month, day
) VALUES 
(
    'track6', 'Song Six', 85, 210000, FALSE, ARRAY['Artist A'], ARRAY['idA'], '2024-06-15', 'High', 0.8, 5, -6.2, TRUE,
    0.05, 0.1, 0.0, 0.12, 0.75, 120.5, 4, 2024, 6, 15
),
(
    'track7', 'Song Seven', 85, 210000, FALSE, ARRAY['Artist A'], ARRAY['idA'], '1922', 'High', 0.8, 5, -6.2, TRUE,
    0.05, 0.1, 0.0, 0.12, 0.75, 120.5, 4, 1922, null, null
);

-- Inserting the artists into the artists table
INSERT INTO music.artists (id, followers, genres, name, popularity)
VALUES
('idA', 10000, ARRAY['Pop', 'Rock'], 'Artist A', 80),
('idB', 5000, ARRAY['Hip-Hop'], 'Artist B', 75),
('idC', 20000, ARRAY['Classical'], 'Name "Nickname" Surname', 90),
('idD', 15000, ARRAY['Indie'], 'Boy''s Choir', 65),
('idE', 15000, ARRAY['Indie'], 'artist 1', 65),
('idF', 120, null, 'artist''s brother', 10),
('idG', 11000, ARRAY['Pop'], 'my "special" artist', 40),
('idH', 12000, null, 'Solo artist', 42);

-- Truncate tables, if needed
-- TRUNCATE TABLE music.artists;
-- TRUNCATE TABLE music.tracks;

