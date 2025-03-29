import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import * as file from './file_handling';
import * as trans from './transformations';

const BUCKET_NAME = 'auma-spotify';
const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
const ARTISTS_FILENAME = 'artists.json';
const TRACKS_FILENAME = 'tracks.json';

// First function of the main flow - processing tracks.csv
// Returns a set of artists that have tracks in the filtered file
async function processTracks(s3: S3Client) {
  console.log('Downloading tracks CSV...');
  const tracks = await file.downloadAndExtractCSV(TRACKS_URL);

  console.log('Filtering tracks...');
  let filteredTracks = trans.filterTracks(tracks);

  console.log('Processing release dates and danceability...');
  filteredTracks = filteredTracks.map((item) => {
    const updatedItem: any = { ...item };

    trans.explodeDateField(updatedItem, 'release_date');  // Explode release date
    trans.stringifyDanceability(updatedItem);

    return updatedItem;
  });

  console.log('Uploading tracks to S3...');
  await file.uploadJSONToS3(TRACKS_FILENAME, filteredTracks, BUCKET_NAME, s3);

  // Extract artists
  const artistsSet = new Set(filteredTracks.flatMap(track => track.artists));

  // Free memory as we're not going to use tracks data anymore
  console.log('Releasing memory...');
  filteredTracks.length = 0;
  tracks.length = 0;

  return artistsSet;
}

// Second function of the main flow - processing artists.csv
// Using the set of artists from tracks
async function processArtists(artistsInTracks, s3: S3Client) {
  // Download CSV files
  const artists = await file.downloadAndExtractCSV(ARTISTS_URL);
  console.log('CSV file downloaded');

  // Filter the artists based on the filtered tracks
  let filteredArtists = trans.filterArtists(artists, artistsInTracks);
  console.log('Artists filtered');

  // Upload the filtered tracks file to AWS S3
  await file.uploadJSONToS3(ARTISTS_FILENAME, filteredArtists, BUCKET_NAME, s3);

}

async function main() {

  // Initialize AWS S3 Client (v3)
  const s3 = new S3Client({
    region: 'eu-north-1',
  });

  try {
    // Work with tracks file (as it's used as input to the artists file)
    const artistsInTracks = await processTracks(s3);

    // Process artists after tracks are cleared
    await processArtists(artistsInTracks, s3);

  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();
