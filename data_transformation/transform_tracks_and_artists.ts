import { S3Client } from '@aws-sdk/client-s3';
import * as file from './file_handling';
import * as trans from './transformations';
import { S3_BUCKET_NAME, ARTISTS_URL, TRACKS_URL, ARTISTS_FILENAME, TRACKS_FILENAME, S3_CONFIG } from './config';

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
async function processArtists(artistsInTracks, s3: S3Client) {
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

async function main() {

  // Initialize AWS S3 Client (v3)
  const s3 = new S3Client({
    region: S3_CONFIG.AWS_REGION,
    credentials: {
      accessKeyId: S3_CONFIG.AWS_ACCESS_KEY_ID,
      secretAccessKey: S3_CONFIG.AWS_SECRET_ACCESS_KEY,
    },
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
