import dotenv from "dotenv";
dotenv.config();

export const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
export const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
export const ARTISTS_FILENAME = 'artists.json';
export const TRACKS_FILENAME = 'tracks.json';

export const S3_BUCKET_NAME = 'auma-spotify';

export const S3_CONFIG = {
    AWS_REGION: 'eu-north-1',
    AWS_ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID!,
    AWS_SECRET_ACCESS_KEY: process.env.S3_SECRET_ACCESS_KEY!,
  };