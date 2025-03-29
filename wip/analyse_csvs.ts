import axios from 'axios';
import { Upload } from '@aws-sdk/lib-storage';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';
import * as stream from 'stream';

const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';


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

function getMaxColumnLengths(data: { [key: string]: any }[]): { [key: string]: { length: number, value: any } } {
  const columnMaxLengths: { [key: string]: { length: number, value: any } } = {};

  // Iterate through each row in the provided data
  data.forEach(row => {
    // Iterate through each key (column) in the row
    Object.keys(row).forEach((column) => {
      const value = row[column];

      // Get the length of the value (if it's a string or convert to string)
      const valueLength = value !== null && value !== undefined ? String(value).length : 0;

      // Update the maximum length and the corresponding value for this column
      if (columnMaxLengths[column] === undefined || valueLength > columnMaxLengths[column].length) {
        columnMaxLengths[column] = { length: valueLength, value: value };
      }
    });
  });

  return columnMaxLengths;
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

  const maxLengths = getMaxColumnLengths(filteredTracks);
  console.log(maxLengths);

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

  const maxLengths = getMaxColumnLengths(filteredArtists);
  console.log(maxLengths);

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
