import axios from 'axios';
import * as AWS from 'aws-sdk';
import * as Papa from 'papaparse';
import * as stream from 'stream';

const BUCKET_NAME = 'auma-spotify'; 
const ARTISTS_URL = 'https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=tracks.csv';
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

// This function returns a stream of data instead of data already parsed into a javascript object
// Useful for large files, but then should have parsing added as well maybe?
// Fetch the CSV file from the URL
/*
async function fetchCSV(url: string): Promise<stream.Readable> {
  const response = await axios.get(url, {
    responseType: 'stream',
  });
  return response.data;
}
  */

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
async function uploadCsvToS3(fileContent: Buffer, fileName: string, bucketName: string): Promise<void> {
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

// Process the CSV file and upload to S3
async function processAndUploadCSV(fileURL: string, fileName: string) {
  try {
    // Fetch the CSV file stream
    const fileStream = await fetchCSV(fileURL);

    // TODO: 
    // 1. Filter tracks file to only have rows where name is not null (not empty) and duration_ms >= 60 000
    // 2. Filter artists file to only have rows where artist_id is in the filtered tracks file
    
    // Optionally, you can parse the CSV here if needed
   // const parsedData: any[] = [];
    
   // fileStream.pipe(csvParser())
    //  .on('data', (row) => {
        // Process each row of the CSV, if needed
    //    parsedData.push(row);
    //  })
     // .on('end', async () => {
        // After parsing, you can choose to upload the CSV directly
        // or manipulate the data before uploading

        // const fileName = 'your-csv-file.csv';  // Give the file a name when uploading to S3
        await uploadToS3(fileStream, fileName);
     // });
  } catch (error) {
    console.error('Error processing CSV file:', error);
  }
}

processAndUploadCSV(ARTISTS_URL, ARTISTS_FILENAME);
processAndUploadCSV(TRACKS_URL, TRACKS_FILENAME);
