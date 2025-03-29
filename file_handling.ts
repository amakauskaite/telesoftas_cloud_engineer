import axios from 'axios';
import * as Papa from 'papaparse';
import * as JSZip from 'jszip';
import { Upload } from '@aws-sdk/lib-storage';
import * as stream from 'stream';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

// Download and extract CSV from a ZIP folder
// Asuming that the ZIP folder only holds the one file we need
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

// For perfomance, a stream is used to upload data to S3
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