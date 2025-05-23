import axios from 'axios';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';
import { Upload } from '@aws-sdk/lib-storage';
import * as stream from 'stream';
import { S3Client } from '@aws-sdk/client-s3';
import * as trans from './transformations';

// Download and extract CSV from a ZIP folder
// Assuming that the ZIP folder only holds the one file we need
export async function downloadAndExtractCSV(url: string): Promise<any[]> {
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
            transform: trans.transformCSVField,
            complete: (result) => resolve(result.data),
            error: (error) => reject(`Error parsing CSV: ${error.message}`),
        });
    });
}

// For performance, a stream is used to upload data to S3
class JSONReadableStream extends stream.Readable {
    private index: number;
    private jsonStarted: boolean;
    private data: any[];

    constructor(data: any[]) {
        super();
        this.index = 0;
        this.jsonStarted = false;
        this.data = data;
    }

    _read() {
        if (!this.jsonStarted) {
            this.push("["); // Start JSON array
            this.jsonStarted = true;
        }

        if (this.index < this.data.length) {
            const chunk = JSON.stringify(this.data[this.index]);

            if (this.index > 0) {
                this.push("," + chunk); // Add commas between objects
            } else {
                this.push(chunk);
            }

            this.index++;
        } else {
            this.push("]"); // Close JSON array
            this.push(null); // End stream
        }
    }
}

export async function uploadJSONToS3(fileName: string, fileContent: any[], bucketName: string, s3: S3Client) {
    const dataStream = new JSONReadableStream(fileContent);

    const upload = new Upload({
        client: s3,
        params: {
            Bucket: bucketName,
            Key: fileName,
            Body: dataStream,
            ContentType: 'application/json',
        },
    });

    await upload.done();
    console.log(`Successfully uploaded ${fileName} to S3`);
}