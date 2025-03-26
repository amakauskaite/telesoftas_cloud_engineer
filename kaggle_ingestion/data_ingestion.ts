import axios from 'axios';
import * as AWS from 'aws-sdk';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';

const BUCKET_NAME = 'auma-spotify'; 

const ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
const ARTISTS_FILENAME = 'artists.csv'
const TRACKS_FILENAME = 'tracks.csv'

// Initialize AWS S3
// Using an IAM user with AWS Toolkit to store credentials, so they're not being passed here in the constructor
const s3 = new AWS.S3();

// Helper function to parse csv data using papaparse
// A special case is handled for artists field that has arrays
function parseCSVData(csvData: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    Papa.parse(csvData, {
      header: true,
      dynamicTyping: true, // Automatically convert values like numbers
      transform: (value, field) => {
        if (field === 'artists') {
          try {
            // Step 1: Remove square brackets
            const valueWithoutBrackets = value.replace(/[\[\]]/g, '');

            // Step 2: Escape commas inside quoted strings by replacing with a placeholder
            const valueWithEscapedCommas = valueWithoutBrackets.replace(/"([^"]*)"/g, (match) => {
              return match.replace(/,/g, '\\comma\\');
            });

            // Step 3: Match all quoted strings and non-quoted parts, split by commas outside quotes
            let artistsArray = valueWithEscapedCommas.match(/'([^']|\\')*'|"([^"]|\\")*"|[^,]+/g)
              .map(item => item.trim().replace(/^['"]|['"]$/g, '')); // Remove surrounding quotes and trim spaces

            // Step 4: Revert the escaped commas back to real commas
            artistsArray = artistsArray.map(item => item.replace(/\\comma\\/g, ','));

            return artistsArray; // Return as an array
          } catch (e) {
            console.error('Parsing error for value:', JSON.stringify(value), e);
            return value; // If it fails, return the original value (can handle cases where it's not a valid array string)
          }
        }
        return value; // Leave other fields unchanged
      },
      complete: (result) => {
        resolve(result.data); // Resolving the promise with parsed data
      },
      error: (error) => {
        reject(`Error parsing CSV: ${error.message}`);
      },
    });
  });
}

// Helper function to download and parse CSV from a ZIP file
async function downloadAndExtractCSV(url: string): Promise<any[]> {
  const response = await axios.get(url, { responseType: 'arraybuffer' }); // Download ZIP as binary

  // Create a new JSZip instance to handle the ZIP file
  const zip = await JSZip.loadAsync(response.data);

  // Assuming there is only one CSV file inside the ZIP, find the first CSV file
  const csvFileName = Object.keys(zip.files).find(fileName => fileName.endsWith('.csv'));

  if (!csvFileName) {
    throw new Error('No CSV file found in the ZIP archive.');
  }

  // Extract the CSV file content
  const csvFile = zip.files[csvFileName];
  const csvData = await csvFile.async('text'); // Get the file content as text

  // Parse the CSV content using a helper function
  return parseCSVData(csvData);
}

// Filter tracks file
// Should only have rows where there's a track name AND the tracks is longer than 1 minute (or 1 minute long)
function filterTracks(tracks: any[]): any[] {
  try {
    return tracks.filter((track) => track.name !== null && track.duration_ms >= 60000);
  } catch (error) {
    console.error('Error:', error);
    return tracks;
  }
  
}

// Filter artists file to only have artists with tracks in the filtered tracks file
function filterArtists(artists: any[], artistsFromTracks: Set<string>): any[] {
  // For each artist in artists file check if if the artist's name is in the list of artistsFromTracks
  try {
    return artists.filter((artist) => artistsFromTracks.has(artist.name)); 
  } catch (error) {
    console.error('Error:', error);
    return artists;
  }
  
}

// TODO: make sure this interface makes sense and is useful
interface DynamicJson {
  [key: string]: any; // Allows any key with any value
}

// Assign undefined if the month and/or day is missing
function assignDateValues(dateParts: string[], updatedJson: DynamicJson) {
  updatedJson['year'] = parseInt(dateParts[0], 10);
  updatedJson['month'] = dateParts[1] ? parseInt(dateParts[1], 10) : null;
  updatedJson['day'] = dateParts[2] ? parseInt(dateParts[2], 10) : null;
}

// Add year, month, day fields to provided json for the date field passed as dateFieldName
function explodeDateFieldsInJson(json: DynamicJson[], dateFieldName: string): DynamicJson[] {
  return json.map((item) => {
      const updatedItem: DynamicJson = { ...item }; // Create a shallow copy to avoid mutating the original object

      const dateField = item[dateFieldName]; // Get the release date from the current object
      // Check if the release date exists and is not undefined or null
      if (dateField != null) {
          let dateParts: string[];

          // If the release date is a string, split it by '-'
          // TODO: make sure actual dates and not hyphenated strings are passed
          if (typeof dateField === 'string') {
              dateParts = dateField.split('-');
          }
          // If the release date is a number (just the year), handle it accordingly
          else if (typeof dateField === 'number') {
              dateParts = [dateField.toString()]; // Treat it as just a year
          } else {
              dateParts = []; // If the release date is neither string nor number, we can't process it
          }

          // Call the function to assign values to the updatedItem
          assignDateValues(dateParts, updatedItem);
      }

      return updatedItem; // Return the modified item
  });
}

// Upload CSV file to S3
async function uploadCSVToS3( fileName: string, fileContent: Buffer, bucketName: string): Promise<void> {
  const params = {
    Bucket: bucketName,
    Key: fileName,
    Body: fileContent,
    ContentType: 'text/csv',
  };

  try {
    await s3.upload(params).promise();
    console.log(`Successfully uploaded ${fileName} to S3`);
  } catch (error) {
    console.error('Error uploading file to S3:', error);
  }
}

// Main function to download, filter, and upload the CSV files
async function main() {
  try {

    // Download CSV files
    const tracks = await downloadAndExtractCSV(TRACKS_URL);
    const artists = await downloadAndExtractCSV(ARTISTS_URL);

    // Filter the first CSV file
    const filteredTracks = filterTracks(tracks);
    
    // Take only the artists that appear on the filtered tracks
    const artistsWithTracks = new Set(filteredTracks.flatMap(track => track.artists));

    // Filter the second CSV file based on artists from the filtered first file
    const filteredArtists = filterArtists(artists, artistsWithTracks);

  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();