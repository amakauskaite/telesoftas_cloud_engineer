CREATE OR REPLACE VIEW views.followed_artist_tracks
AS
-- Filtering to have less data scanned when joining
WITH artists_with_followers  AS
(
	SELECT
		id as artist_id,
		name as artist_name
	FROM music.artists
	WHERE followers > 0
)
SELECT
	artist_id,
	artist_name,
	t.id as track_id,
	t.name as track_name
FROM artists_with_followers a
JOIN music.tracks t ON a.artist_id = ANY(t.id_artists)
ORDER BY artist_id
;