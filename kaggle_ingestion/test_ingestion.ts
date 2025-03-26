import axios from 'axios';
import { S3Client, PutObjectCommand} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';
import * as stream from 'stream';
import * as JSONStream from 'JSONStream';

const BUCKET_NAME = 'auma-spotify';
const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
const ARTISTS_FILENAME = 'artists.json';
const TRACKS_FILENAME = 'tracks.json';

// Initialize AWS S3 Client (v3)
const s3 = new S3Client({
  region: 'eu-north-1',
  credentials: {
    accessKeyId: 'AKIAX5T2WSIPGYLZQKQO',
    secretAccessKey: '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',
  },
});

// Helper function to download and extract CSV from ZIP file
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

// Filter tracks to only include valid tracks (with name and duration >= 60 seconds)
function filterTracks(data: any[]): any[] {
  return data.filter((row) => row.name !== null && row.duration_ms >= 60000);
}

// Filter artists to only include those who have tracks in the filtered tracks list
function filterArtists(artists: any[], artistsFromTracks: Set<string>): any[] {
  return artists.filter((artist) => artistsFromTracks.has(artist.name));
}

// Helper function to assign year, month, and day from date string
function assignDateValues(dateParts: string[], updatedJson: any) {
  const [year, month = null, day = null] = dateParts.map(part => part ? parseInt(part, 10) : null);
  updatedJson['year'] = year;
  updatedJson['month'] = month;
  updatedJson['day'] = day;
}

// Explode the date field into separate year, month, and day fields
function explodeDateFieldsInJson(json: any[], dateFieldName: string): any[] {
  return json.map((item) => {
    const updatedItem = { ...item };
    const dateField = item[dateFieldName];

    if (dateField) {
      // Always convert the dateField to a string and split by '-'
      const dateParts = dateField.toString().split('-');

      // Assign the extracted values (year, month, day) to the updatedItem
      assignDateValues(dateParts, updatedItem);
    } else {
      // Handle cases where dateField is invalid or missing
      updatedItem['year'] = null;
      updatedItem['month'] = null;
      updatedItem['day'] = null;
    }

    return updatedItem;
  });
}

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
      Body: dataStream,  // Proper streaming
      ContentType: 'application/json',
    },
  });

  await upload.done();
  console.log(`Successfully uploaded ${fileName} to S3`);
}

async function processTracks() {
  console.log('Downloading tracks CSV...');
  const tracks = await downloadAndExtractCSV(TRACKS_URL);
  
  console.log('Filtering tracks...');
  let filteredTracks = filterTracks(tracks);

  console.log('Processing release dates...');
  filteredTracks = explodeDateFieldsInJson(filteredTracks, 'release_date');

  console.log('Uploading tracks to S3...');
  await uploadJSONToS3(TRACKS_FILENAME, filteredTracks, BUCKET_NAME);

  // Extract artists
  const artistsSet = new Set(filteredTracks.flatMap(track => track.artists));

  // Free memory (set large variables to null)
  console.log('Releasing memory...');
  filteredTracks.length = 0;
  tracks.length = 0;

  global.gc?.(); // Force garbage collection (if allowed)

  return artistsSet;
}

async function processArtists(artistsInTracks)  {
  // Download CSV files
  const artists = await downloadAndExtractCSV(ARTISTS_URL);
  console.log('CSV file downloaded');

  // Filter the artists based on the filtered tracks
  let filteredArtists = filterArtists(artists, artistsInTracks);
  console.log('Artists filtered');

  // Upload the filtered tracks file to AWS S3
  await uploadJSONToS3(ARTISTS_FILENAME, filteredArtists, BUCKET_NAME);

}

// Main function to download, filter, and upload the CSV files
async function main() {
  try {
    // Work with tracks file (as it's used as input to the artists file)
    const artistsInTracks = await processTracks();

    //Clean memory after processing tracks
    console.log('Cleaning up memory...');
    global.gc?.(); // Force garbage collection (only works if --expose-gc flag is enabled)
    
    // Process artists after tracks are cleared
    await processArtists(artistsInTracks);

    // Work with artists file
    // const artists = await downloadAndExtractCSV(ARTISTS_URL);
    // console.log('CSV file downloaded');

    // Filter the artists based on the filtered tracks
    // let filteredArtists = filterArtists(artists, artistsWithTracks);
    // console.log('Artists filtered');
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();
