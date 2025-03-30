CREATE OR REPLACE VIEW views.track_overview
AS
SELECT 
    t.id AS track_id,
    t.name,
    t.popularity,
    t.energy,
	t.danceability,
	sum(a.followers) as total_artist_followers
FROM music.tracks t
JOIN LATERAL unnest(t.id_artists) artist_id ON TRUE
JOIN music.artists a ON artist_id = a.id
GROUP BY t.id, t.name, t.popularity, t.energy, t.danceability
;