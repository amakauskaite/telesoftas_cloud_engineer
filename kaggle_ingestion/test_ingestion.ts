import axios from 'axios';
import * as AWS from 'aws-sdk';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';

const BUCKET_NAME = 'auma-spotify'; 

const ARTISTS_URL = 'https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=artists.csv';
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
      dynamicTyping: true,
      complete: (results) => resolve(results.data),
      error: (err) => reject(err),
    });
  });
}

// Filter tracks file
// Should only have rows where there's a track name AND the tracks is longer than 1 minute (or 1 minute long)
function filterTracks(data: any[]): any[] {
  return data.filter((row) => row.name !== null && row.duration_ms >= 60000);
}

// Filter artists file to only have artists with tracks in the filtered tracks file
function filterArtists(data: any[], artists: string[]): any[] {
  return data.filter((row) => artists.includes(row.artist));
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
    console.log('File downloaded');
    console.log('Row count:', tracks.length)
    console.log(tracks[0])
    // console.log(tracks[1])
    /*
    const artists = await downloadCSV(ARTISTS_URL);
    */

    // Filter the first CSV file
    const filteredTracks = filterTracks(tracks);
    console.log('File filtered');
    console.log('Row count:', filteredTracks.length)

    /*

    // Extract artist names from the filtered first file
    const artistsWithTracks = filteredTracks.map((row) => row.artist);

    // Filter the second CSV file based on artists from the filtered first file
    const filteredArtists = filterArtists(artists, artistsWithTracks);

    // Convert filtered data back to CSV format
    const filteredTracksCSV = Papa.unparse(filteredTracks);
    const filteredArtistsCSV = Papa.unparse(filteredArtists);

    // Upload the filtered CSV files to AWS S3
    await uploadCSVToS3(TRACKS_FILENAME, Buffer.from(filteredTracksCSV), BUCKET_NAME);
    await uploadCSVToS3(ARTISTS_FILENAME, Buffer.from(filteredArtistsCSV), BUCKET_NAME);

    console.log('Files uploaded successfully to S3');
    */
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();