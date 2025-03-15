import axios from 'axios';
import * as AWS from 'aws-sdk';
// import * as csvParser from 'csv-parser';
import * as stream from 'stream';

// Initialize AWS S3
const s3 = new AWS.S3();
const BUCKET_NAME = 'auma-spotify'; 
const ARTISTS_URL = 'https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=artists.csv';
const TRACKS_URL = 'https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=tracks.csv';
const ARTISTS_FILENAME = 'artists.csv'
const TRACKS_FILENAME = 'tracks.csv'

// Fetch the CSV file from the URL
async function fetchCSV(url: string): Promise<stream.Readable> {
  const response = await axios.get(url, {
    responseType: 'stream',
  });
  return response.data;
}

// Upload to S3
async function uploadToS3(fileStream: stream.Readable, fileName: string): Promise<void> {
  const params = {
    Bucket: BUCKET_NAME,
    Key: fileName,
    Body: fileStream,
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
