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

// Helper function to download and parse CSV from URL
async function downloadCSV(url: string): Promise<any[]> {
  const response = await axios.get(url);
  return new Promise((resolve, reject) => {
    Papa.parse(response.data, {
      header: true,
      dynamicTyping: true,
      complete: (results) => resolve(results.data),
      error: (err) => reject(err),
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

  // Parse the CSV content using PapaParse
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

// Filter tracks file
// Should only have rows where there's a track name AND the tracks is longer than 1 minute (or 1 minute long)
function filterTracks(data: any[]): any[] {
  console.log("Null row cnt:", data.filter((row) => row.name === null).length, "short song row cnt:", data.filter((row) => row.duration_ms < 60000).length)
  return data.filter((row) => row.name !== null && row.duration_ms >= 60000);
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

// TODO: move helper functions to a separate file. Make them reusable, if possible
// Main function to download, filter, and upload the CSV files
async function main() {
  try {

    // Download CSV files
    const tracks = await downloadAndExtractCSV(TRACKS_URL);
    console.log('1. File downloaded');
    console.log('Row count:', tracks.length);
    // console.log(tracks[0])

    const artists = await downloadAndExtractCSV(ARTISTS_URL);
    console.log('2. File downloaded');
    console.log('Row count:', tracks.length);
    // console.log(artists[0])

    // Filter the first CSV file
    const filteredTracks = filterTracks(tracks);
    console.log('3. File filtered');
    console.log('Row count:', filteredTracks.length);
    console.log(filteredTracks[0].artists);
    console.log(filteredTracks[827].artists);

    const artistsWithTracks = new Set(filteredTracks.flatMap(track => track.artists));
    console.log('4. Artists with tracks taken');
    console.log('Row count:', artistsWithTracks.values.length);
    // console.log("Is first artist Uli?", artistsWithTracks[0] === 'Uli')
    // console.log(artistsWithTracks[827]);


    // Filter the second CSV file based on artists from the filtered first file
    const filteredArtists = filterArtists(artists, artistsWithTracks);
    console.log('5. File filtered');
    console.log('Row count:', filteredArtists.length)
    // console.log(filteredArtists[0])

     
    // Convert filtered data back to CSV format
    const filteredTracksCSV = Papa.unparse(filteredTracks);
    console.log('6. tracks converted to csv');
    console.log(filteredTracksCSV[0])

    const filteredArtistsCSV = Papa.unparse(filteredArtists);
    console.log('7. artists converted to csv');
    console.log(filteredArtistsCSV[0])

    // Upload the filtered CSV files to AWS S3
    await uploadCSVToS3(TRACKS_FILENAME, Buffer.from(filteredTracksCSV), BUCKET_NAME);
    await uploadCSVToS3(ARTISTS_FILENAME, Buffer.from(filteredArtistsCSV), BUCKET_NAME);

    console.log('Files uploaded successfully to S3');
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();