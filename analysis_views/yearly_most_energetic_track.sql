CREATE VIEW yearly_most_energetic_track
AS
SELECT
    release_year,
    track_id,
    track_name,
    track_energy
FROM
(
    -- NOTE: this syntax is PostgreSQL specific
    -- In other dialects, it should be substituted to a window function
    SELECT DISTINCT ON (year)
	    year as release_year,
	    id as track_id,
	    name as track_name,
	    energy as track_energy
    FROM music.tracks
    ORDER BY year, energy DESC
) A;