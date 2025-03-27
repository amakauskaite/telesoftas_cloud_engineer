import axios from 'axios';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';
import * as stream from 'stream';

const BUCKET_NAME = 'auma-spotify';
const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
const ARTISTS_FILENAME = 'artists.csv';
const TRACKS_FILENAME = 'tracks.csv';

// Initialize AWS S3 Client (v3)
const s3 = new S3Client({
  region: 'eu-north-1',
});

// Download and extract CSV from a ZIP folder
// Assuming that the ZIP folder only holds the one file we need
async function downloadAndExtractCSV(url: string): Promise<any[]> {
  const response = await axios.get(url, { responseType: 'arraybuffer' });
  const zip = await JSZip.loadAsync(response.data);
  const csvFileName = Object.keys(zip.files).find(fileName => fileName.endsWith('.csv'));

  if (!csvFileName) {
    throw new Error('No CSV file found in the ZIP archive.');
  }

  const csvFile = zip.files[csvFileName];
  const csvData = await csvFile.async('text');

  return new Promise((resolve, reject) => {
    Papa.parse(csvData, {
      header: true,
      dynamicTyping: true,
      transform: (value, field) => {
        if (field === 'artists') {
          // Reverse transformation for the 'artists' field
          let cleanedValue = value.replace(/\\comma\\/g, ',');
          const artistsArray = cleanedValue.split(',')
            .map(item => item.trim().replace(/^['"]|['"]$/g, ''));
          return `"${artistsArray.join(',')}"`; // Join back into a string with commas separating artists
        }
        return value;
      },
      complete: (result) => resolve(result.data),
      error: (error) => reject(`Error parsing CSV: ${error.message}`),
    });
  });
}

// Filter tracks to only include valid tracks (with a name and duration >= 60 seconds)
function filterTracks(data: any[]): any[] {
  return data.filter((row) => row.name !== null && row.duration_ms >= 60000);
}

// Filter artists to only include those who have tracks in the filtered tracks list
function filterArtists(artists: any[], artistsFromTracks: Set<string>): any[] {
  return artists.filter((artist) => artistsFromTracks.has(artist.name));
}

// Create year, month, day fields; assign undefined if the month and/or day is missing
function parseDateParts(dateParts: string[]): { year: number | null; month: number | null; day: number | null } {
  const [year, month = null, day = null] = dateParts.map(part => (part ? parseInt(part, 10) : null));
  return { year, month, day };
}

// Generic function to explode a date field into year, month, and day
function explodeDateField(updatedJson: any, dateField: string) {
  if (updatedJson[dateField] != null) {
    const dateParts = String(updatedJson[dateField]).split('-');
    const { year, month, day } = parseDateParts(dateParts);

    updatedJson['year'] = year;
    updatedJson['month'] = month;
    updatedJson['day'] = day;
  }
}

// Update danceability value
function stringifyDanceability(updatedJson: any) {
  if (updatedJson['danceability'] >= 0 && updatedJson['danceability'] < 0.5) {
    updatedJson['danceability'] = 'Low';
  } else if (updatedJson['danceability'] >= 0.5 && updatedJson['danceability'] <= 0.6) {
    updatedJson['danceability'] = 'Medium';
  } else if (updatedJson['danceability'] > 0.6 && updatedJson['danceability'] <= 1) {
    updatedJson['danceability'] = 'High';
  } else {
    updatedJson['danceability'] = 'Undefined';
  }
}

// Convert data to CSV string and upload to S3
async function uploadCSVToS3(fileName: string, fileContent: any[], bucketName: string) {
  const csvContent = Papa.unparse(fileContent);

  const upload = new Upload({
    client: s3,
    params: {
      Bucket: bucketName,
      Key: fileName,
      Body: csvContent,
      ContentType: 'text/csv',
    },
  });

  await upload.done();
  console.log(`Successfully uploaded ${fileName} to S3`);
}

// First function of the main flow - processing tracks.csv
// Returns a set of artists that have tracks in the filtered file
async function processTracks() {
  console.log('Downloading tracks CSV...');
  const tracks = await downloadAndExtractCSV(TRACKS_URL);

  console.log('Filtering tracks...');
  let filteredTracks = filterTracks(tracks);

  console.log('Processing release dates and danceability...');
  filteredTracks = filteredTracks.map((item) => {
    const updatedItem: any = { ...item };

    explodeDateField(updatedItem, 'release_date');  // Explode release date
    stringifyDanceability(updatedItem);

    return updatedItem;
  });

  console.log('Uploading tracks to S3...');
  await uploadCSVToS3(TRACKS_FILENAME, filteredTracks, BUCKET_NAME);

  // Extract artists
  const artistsSet = new Set(filteredTracks.flatMap(track => track.artists));

  console.log('Releasing memory...');
  filteredTracks.length = 0;
  tracks.length = 0;

  return artistsSet;
}

// Second function of the main flow - processing artists.csv
// Using the set of artists from tracks
async function processArtists(artistsInTracks) {
  console.log('Downloading artists CSV...');
  const artists = await downloadAndExtractCSV(ARTISTS_URL);
  console.log('CSV file downloaded');

  let filteredArtists = filterArtists(artists, artistsInTracks);
  console.log('Artists filtered');

  await uploadCSVToS3(ARTISTS_FILENAME, filteredArtists, BUCKET_NAME);
}

async function main() {
  try {
    // Process tracks first (as artists depend on the tracks)
    const artistsInTracks = await processTracks();

    // Process artists after tracks
    await processArtists(artistsInTracks);

  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();
