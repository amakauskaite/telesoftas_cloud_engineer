-- Create tables
/*
** NOTE 1: all floating point values (for e.g. energy, liveliness, etc) are created as DOUBLE (float8)
** and not decimal(p, s) based on the fact that the precision after the decimal point
** in the data varies (in some cases can go up to 8 decimals after 0, for example instrumentalness value 0.00000154)
** so most likely we're not worried about exact calculations for these values.

** NOTE 2: values containing zeroes and ones (i.e. explicit and more) have been changed to booleans
** to use less storage place and leave less space for interpretation for what these columns describe.
*/
CREATE TABLE music.tracks (
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

CREATE TABLE music.artists (
	id TEXT PRIMARY KEY,
	followers INT,
	genres TEXT[],
	name TEXT,
	popularity SMALLINT
);