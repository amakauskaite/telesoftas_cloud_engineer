import axios from 'axios';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';  // Import S3Client and PutObjectCommand from AWS SDK v3
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';

const BUCKET_NAME = 'auma-spotify';
const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
const ARTISTS_FILENAME = 'artists.csv';
const TRACKS_FILENAME = 'tracks.csv';

// Initialize AWS S3 Client (v3)
const s3 = new S3Client({
  region: 'eu-north-1', // Replace with the appropriate region
});

// Helper function to download and parse CSV from URL
async function downloadAndParseCSV(url: string): Promise<any[]> {
  const response = await axios.get(url);
  return new Promise((resolve, reject) => {
    Papa.parse(response.data, {
      header: true,
      dynamicTyping: true,
      complete: (result) => resolve(result.data),
      error: (error) => reject(`Error parsing CSV: ${error.message}`),
    });
  });
}

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

// Upload CSV file to S3 (v3)
async function uploadCSVToS3(fileName: string, fileContent: Buffer, bucketName: string): Promise<void> {
  const params = {
    Bucket: bucketName,
    Key: fileName,
    Body: fileContent,
    ContentType: 'text/csv',
  };

  try {
    const command = new PutObjectCommand(params);  // Using PutObjectCommand from AWS SDK v3
    await s3.send(command);  // Using the send() method to execute the command
    console.log(`Successfully uploaded ${fileName} to S3`);
  } catch (error) {
    console.error('Error uploading file to S3:', error);
  }
}

// Main function to download, filter, and upload the CSV files
async function main() {
  try {
    // Download CSV files concurrently
    const [tracks, artists] = await Promise.all([
      downloadAndExtractCSV(TRACKS_URL),
      downloadAndExtractCSV(ARTISTS_URL),
    ]);
    console.log('CSV files downloaded');

    // Filter the tracks (only valid tracks)
    let filteredTracks = filterTracks(tracks);
    console.log('Tracks filtered');

    // Extract artists from filtered tracks
    const artistsWithTracks = new Set(filteredTracks.flatMap(track => track.artists));

    // Filter the artists based on the filtered tracks
    let filteredArtists = filterArtists(artists, artistsWithTracks);
    console.log('Artists filtered');

    // Explode the date fields in tracks
    filteredTracks = explodeDateFieldsInJson(filteredTracks, 'release_date');
    console.log('Date fields exploded in tracks');

    /*
    // Convert filtered data back to CSV format (if necessary)
    const filteredTracksCSV = Papa.unparse(filteredTracks);
    const filteredArtistsCSV = Papa.unparse(filteredArtists);

    // Upload the filtered CSV files to AWS S3
    await Promise.all([
      uploadCSVToS3(TRACKS_FILENAME, Buffer.from(filteredTracksCSV), BUCKET_NAME),
      uploadCSVToS3(ARTISTS_FILENAME, Buffer.from(filteredArtistsCSV), BUCKET_NAME),
    ]);

    console.log('Files uploaded successfully to S3');
    */
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();
