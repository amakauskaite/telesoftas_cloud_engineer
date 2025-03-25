import * as fs from 'fs';
import * as Papa from 'papaparse';

function parseCSVFileFromPath(filePath: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    // Read the file from the local filesystem
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        reject(`Error reading file: ${err}`);
        return;
      }

      Papa.parse(data, {
        header: true,
        dynamicTyping: true, // Automatically convert values like numbers
        transform: (value, field) => {
          if (field === 'artists') {
            try {                
                const cleanedValue = JSON.parse(JSON.stringify(value.replace(/[\[\]]/g,'')));
                let artistsArray = cleanedValue.match(/'[^']+'|"[^"]+"|[^,]+/g).map(item => item.trim().replace(/^['"]|['"]$/g, ''));

              console.log("Original value: <",JSON.stringify(value),">")
              console.log("Cleaned value: <",JSON.stringify(value),">")
              console.log("Array value: <",JSON.stringify(artistsArray),">")
              // console.log("Cleaned value interim: <",JSON.stringify(cleanedValue0),">")
              // console.log("Cleaned value final: <",JSON.stringify(value.replace(/[\[\]]/g,'')),">")
              return artistsArray; // Converts stringified arrays into actual arrays
            } catch (e) {
              console.error('Parsing error for value:', JSON.stringify(value), e);
              return value; // If it fails, return the original value (can handle cases where it's not a valid array string)
            }
          }
          return value; // Leave other fields unchanged
        },
        complete: (result) => {
          // console.log(result.data); // Your parsed CSV data
          resolve(result.data); // Resolving the promise with parsed data
        },
        error: (error) => {
          reject(`Error parsing CSV: ${error.message}`);
        },
      });
    });
  });
}

// Example usage
const filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\complex_artists_examples.csv';

parseCSVFileFromPath(filePath)
  .then((data) => {
    console.log('Parsed CSV data:', data);
    console.log(data[0].artists[0])
  })
  .catch((error) => {
    console.error(error);
  });
