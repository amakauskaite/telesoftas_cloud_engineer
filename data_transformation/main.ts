import { S3Client } from '@aws-sdk/client-s3';
import { S3_CONFIG } from './config';
import * as file from './fileProcessing';

async function main() {

  // Initialize AWS S3 Client (v3)
  const s3 = new S3Client({
    region: S3_CONFIG.AWS_REGION,
    credentials: {
      accessKeyId: S3_CONFIG.AWS_ACCESS_KEY_ID,
      secretAccessKey: S3_CONFIG.AWS_SECRET_ACCESS_KEY,
    },
  });

  try {
    // Work with tracks file (as it's used as input to the artists file)
    const artistsInTracks = await file.processTracks(s3);

    // Process artists after tracks are cleared
    await file.processArtists(artistsInTracks, s3);

  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the main function
main();
