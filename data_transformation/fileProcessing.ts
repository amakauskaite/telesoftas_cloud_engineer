import { S3Client } from '@aws-sdk/client-s3';
import * as file from './fileHandling';
import * as trans from './transformations';
import { S3_BUCKET_NAME, ARTISTS_URL, TRACKS_URL, ARTISTS_FILENAME, TRACKS_FILENAME} from './config';

// First function of the main flow - processing tracks.csv
// Returns a set of artists that have tracks in the filtered file
export async function processTracks(s3: S3Client) {
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
  await file.uploadJSONToS3(TRACKS_FILENAME, filteredTracks, S3_BUCKET_NAME, s3);

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
export async function processArtists(artistsInTracks, s3: S3Client) {
  // Download CSV files
  console.log('Downloading artists CSV...');
  const artists = await file.downloadAndExtractCSV(ARTISTS_URL);

  // Filter the artists based on the filtered tracks
  console.log('Filtering artists with tracks...');
  let filteredArtists = trans.filterArtists(artists, artistsInTracks);

  // Upload the filtered tracks file to AWS S3
  console.log('Uploading artists to S3...');
  await file.uploadJSONToS3(ARTISTS_FILENAME, filteredArtists, S3_BUCKET_NAME, s3);

}