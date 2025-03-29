CREATE OR REPLACE VIEW views.followed_artist_tracks
AS
SELECT
	a.id as artist_id,
	a.name as artist_name,
	t.id as track_id,
	t.name as track_name
FROM music.artists a
JOIN music.tracks t ON a.id = ANY(t.id_artists)
WHERE a.followers > 0
ORDER BY artist_id
;