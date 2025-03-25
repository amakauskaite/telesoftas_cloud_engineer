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
      dynamicTyping: true,
      complete: (results) => resolve(results.data),
      error: (err) => reject(err),
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
function filterArtists(artists: any[], artistsFromTracks: string[]): any[] {
  // console.log(data[0])
  console.log("First 5 artists in tracks:", artists[0].name, ", ",artists[1].name,", ", artists[2].name,", ",artists[3].name,", ",artists[4].name)
  console.log("First 5 artists in artists:", artistsFromTracks[0], ", ",artistsFromTracks[1],", ", artistsFromTracks[2],", ",artistsFromTracks[3],", ",artistsFromTracks[4])
  // For each artist in artists file check if if the artist's name is in the list of artistsFromTracks
  return artists.filter((artist) => artistsFromTracks.includes(artist.name));
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
    console.log('Row count:', tracks.length)
    // console.log(tracks[0])

    //const artists = await downloadAndExtractCSV(ARTISTS_URL);
    //console.log('2. File downloaded');
    //console.log('Row count:', tracks.length)
    // console.log(artists[0])

    // Filter the first CSV file
    const filteredTracks = filterTracks(tracks);
    console.log('3. File filtered');
    console.log('Row count:', filteredTracks.length)
    console.log(filteredTracks[0].artists)
    console.log(JSON.parse(filteredTracks[0].artists.replace(/'([^']+)'/g, '"$1"')))
    console.log(filteredTracks[827].artists)
    console.log(JSON.parse(filteredTracks[827].artists.replace(/'([^']+)'/g, '"$1"')))
    /*
    console.log(filteredTracks[0])
    console.log(typeof filteredTracks[0].artists)
    console.log(filteredTracks[827])
    console.log(typeof filteredTracks[827].artists)
    */
    // console.log(filteredTracks[827].artists)


    // Extract artist names from the filtered first file
      // const artistsWithTracks = filteredTracks.map(track => track.artists!== null ? JSON.parse(track.artists.replace(/'([^']+)'/g, '"$1"')));
      const artistsWithTracks = filteredTracks.map(track => {
        try {
          // Ensure track.artists is a string and replace single quotes with double quotes
          return  track.artists!== null ? JSON.parse(track.artists.replace(/'([^',]+)'/g, '"$1"')) : null;
        } catch (error) {
          // Log the error and the problematic track
          console.error("Error parsing artists for track:", track, error);
          throw new Error('Parsing failed on first error'); // This will stop further execution
          // You can also return an empty array or any fallback value here
          // return [];
        }
      });
      console.log(artistsWithTracks[0], ",",artistsWithTracks[827]);
    
    /*
    const artistsWithTracks = Array.from(new Set(
      filteredTracks.flatMap(track =>  JSON.parse(track.artists.replace(/'([^']+)'/g, '"$1"')))));
    console.log('4. Artists with tracks taken');
    console.log('Row count:', artistsWithTracks.length)
    console.log(artistsWithTracks[0], ",",artistsWithTracks[827])
    console.log("Is first artist Uli?", artistsWithTracks[0] === 'Uli')
    console.log("Is first artist ['Uli?']", artistsWithTracks[0] === "['Uli']")
    */
    // console.log("Is Uli in the array?", artistsWithTracksString.includes('Uli'))
    // console.log("Does includes work?", artistsWithTracksString.includes(artistsWithTracksString[0]))
    // console.log(artistsWithTracks)


    // Filter the second CSV file based on artists from the filtered first file
    // const filteredArtists = filterArtists(artists, artistsWithTracks);
    //console.log('5. File filtered');
    //console.log('Row count:', filteredArtists.length)
    //console.log(filteredArtists[0])

     /*
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