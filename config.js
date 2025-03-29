"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.S3_CONFIG = exports.S3_BUCKET_NAME = exports.TRACKS_FILENAME = exports.ARTISTS_FILENAME = exports.TRACKS_URL = exports.ARTISTS_URL = void 0;
var dotenv = require("dotenv");
dotenv.config();
exports.ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
exports.TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
exports.ARTISTS_FILENAME = 'artists.json';
exports.TRACKS_FILENAME = 'tracks.json';
exports.S3_BUCKET_NAME = 'auma-spotify';
exports.S3_CONFIG = {
    AWS_REGION: 'eu-north-1',
    AWS_ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY: process.env.S3_SECRET_ACCESS_KEY,
};
