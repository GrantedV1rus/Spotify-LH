-- 1) Staging table matching CSV headers (all TEXT)
DROP TABLE IF EXISTS personal_spotify.listening_history;
CREATE TABLE personal_spotify.listening_history (
    ts TEXT,
    platform TEXT,
    ms_played TEXT,
    conn_country TEXT,
    ip_addr TEXT,
    master_metadata_track_name TEXT,
    master_metadata_album_artist_name TEXT,
    master_metadata_album_name TEXT,
    spotify_track_uri TEXT,
    episode_name TEXT,
    episode_show_name TEXT,
    spotify_episode_uri TEXT,
    audiobook_title TEXT,
    audiobook_uri TEXT,
    audiobook_chapter_uri TEXT,
    audiobook_chapter_title TEXT,
    reason_start TEXT,
    reason_end TEXT,
    shuffle TEXT,
    skipped TEXT,
    offline TEXT,
    offline_timestamp TEXT,
    incognito_mode TEXT,
    shuffled TEXT
);

SELECT * FROM personal_spotify.listening_history;

-- 2) Copy CSV into table
COPY personal_spotify.listening_history(
    ts,platform,ms_played,conn_country,ip_addr,master_metadata_track_name,
    master_metadata_album_artist_name,master_metadata_album_name,spotify_track_uri,
    episode_name,episode_show_name,spotify_episode_uri,
    audiobook_title,audiobook_uri,audiobook_chapter_uri,audiobook_chapter_title,reason_start,
    reason_end,shuffle,skipped,offline,offline_timestamp,incognito_mode,shuffled
) 
FROM 'C:/Games/temp/combined_streaming_history.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT * FROM personal_spotify.listening_history;


-- 3) Create Final Table
DROP TABLE IF EXISTS personal_spotify.complete_history CASCADE;
CREATE TABLE personal_spotify.complete_history (
    ts TIMESTAMPTZ, -- original 'timestamp'
    platform TEXT,
    ms_played FLOAT,
    conn_country VARCHAR(2),
    ip_addr INET,
    master_metadata_track_name TEXT,
    master_metadata_album_artist_name TEXT,
    master_metadata_album_name TEXT,
    spotify_track_uri TEXT,
    episode_name TEXT,
    episode_show_name TEXT,
    spotify_episode_uri TEXT,
    audiobook_title TEXT,
    audiobook_uri TEXT,
    audiobook_chapter_uri TEXT,
    audiobook_chapter_title TEXT,
    reason_start TEXT,
    reason_end TEXT,
    shuffle BOOLEAN,
    skipped BOOLEAN,
    offline BOOLEAN,
    offline_timestamp FLOAT,
    incognito_mode BOOLEAN,
    shuffled BOOLEAN
);

SELECT * FROM personal_spotify.complete_history;

-- 4) Insert temp into Final Table Casting
INSERT INTO personal_spotify.complete_history (
   ts,platform,ms_played,conn_country,ip_addr,master_metadata_track_name,
    master_metadata_album_artist_name,master_metadata_album_name,spotify_track_uri,
    episode_name,episode_show_name,spotify_episode_uri,
    audiobook_title,audiobook_uri,audiobook_chapter_uri,audiobook_chapter_title,reason_start,
    reason_end,shuffle,skipped,offline,offline_timestamp,incognito_mode,shuffled
)

SELECT
    ts::TIMESTAMPTZ,
    platform::TEXT,
    ms_played::FLOAT,
    conn_country::VARCHAR(2),
    ip_addr::INET,
    master_metadata_track_name::TEXT,
    master_metadata_album_artist_name::TEXT,
    master_metadata_album_name::TEXT,
    spotify_track_uri::TEXT,
    episode_name::TEXT,
    episode_show_name::TEXT,
    spotify_episode_uri::TEXT,
    audiobook_title::TEXT,
    audiobook_uri::TEXT,
    audiobook_chapter_uri::TEXT,
    audiobook_chapter_title::TEXT,
    reason_start::TEXT,
    reason_end::TEXT,
    CASE WHEN trim(shuffle) = 'True' THEN True ELSE False END,
    CASE WHEN trim(skipped) = 'True' THEN True ELSE False END,
    CASE WHEN trim(offline) = 'True' THEN True ELSE False END,
    NULLIF(offline_timestamp, '')::FLOAT,
    CASE WHEN trim(incognito_mode) = 'True' THEN True ELSE False END,
    CASE WHEN trim(shuffled) = 'True' THEN True ELSE False END
FROM personal_spotify.listening_history;

SELECT * FROM personal_spotify.listening_history

--FURTHER NORMALIZATION NEEDED(SNOWFLAKE SCHEMA)
