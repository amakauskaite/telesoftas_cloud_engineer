import axios from 'axios';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';
import * as stream from 'stream';

const BUCKET_NAME = 'auma-spotify';
const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
const ARTISTS_FILENAME = 'artists.json';
const TRACKS_FILENAME = 'tracks.json';

// Initialize AWS S3 Client (v3)
const s3 = new S3Client({
  region: 'eu-north-1',
});

// Download and extract CSV from a ZIP folder
// Asuming that the ZIP folder only holds the one file we need
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
          // Cleaning up the artists field that will be used later
          // We'll need an array of strings not just a string with an array inside
          const cleanedValue = value.replace(/[\[\]]/g, '')
            .replace(/"([^"]*)"/g, (match) => match.replace(/,/g, '\\comma\\'));

          let artistsArray = cleanedValue.match(/'([^']|\\')*'|"([^"]|\\")*"|[^,]+/g)
            .map(item => item.trim().replace(/^['"]|['"]$/g, ''))
            .map(item => item.replace(/\\comma\\/g, ','));
          return artistsArray;
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
    // Explicitly casting to string for cases when there's only the year known
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

// For perfomance, a stream is used to upload data to S3
class JSONReadableStream extends stream.Readable {
  private index: number;
  private jsonStarted: boolean;
  private data: any[];

  constructor(data: any[]) {
    super();
    this.index = 0;
    this.jsonStarted = false;
    this.data = data;
  }

  _read() {
    if (!this.jsonStarted) {
      this.push("["); // Start JSON array
      this.jsonStarted = true;
    }

    if (this.index < this.data.length) {
      const chunk = JSON.stringify(this.data[this.index]);

      if (this.index > 0) {
        this.push("," + chunk); // Add commas between objects
      } else {
        this.push(chunk);
      }

      this.index++;
    } else {
      this.push("]"); // Close JSON array
      this.push(null); // End stream
    }
  }
}

async function uploadJSONToS3(fileName: string, fileContent: any[], bucketName: string) {
  const dataStream = new JSONReadableStream(fileContent);

  const upload = new Upload({
    client: s3,
    params: {
      Bucket: bucketName,
      Key: fileName,
      Body: dataStream, 
      ContentType: 'application/json',
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
  await uploadJSONToS3(TRACKS_FILENAME, filteredTracks, BUCKET_NAME);

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
async function processArtists(artistsInTracks) {
  // Download CSV files
  const artists = await downloadAndExtractCSV(ARTISTS_URL);
  console.log('CSV file downloaded');

  // Filter the artists based on the filtered tracks
  let filteredArtists = filterArtists(artists, artistsInTracks);
  console.log('Artists filtered');

  // Upload the filtered tracks file to AWS S3
  await uploadJSONToS3(ARTISTS_FILENAME, filteredArtists, BUCKET_NAME);

}

async function main() {
  try {
    // Work with tracks file (as it's used as input to the artists file)
    const artistsInTracks = await processTracks();

    // Process artists after tracks are cleared
    await processArtists(artistsInTracks);

  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();
