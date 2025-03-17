import axios from 'axios';
import * as AWS from 'aws-sdk';
import * as Papa from 'papaparse';

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

