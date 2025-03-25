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
              return JSON.parse(value); // Converts stringified arrays into actual arrays
            } catch (e) {
              return value; // If it fails, return the original value (can handle cases where it's not a valid array string)
            }
          }
          return value; // Leave other fields unchanged
        },
        complete: (result) => {
          console.log(result.data); // Your parsed CSV data
        }
      });
      
/*
      // Parse the CSV data using PapaParse
      Papa.parse(data, {
        header: true, // Treat the first row as headers
        dynamicTyping: true, // Automatically convert types (e.g., number, string)
        complete: (result) => {
          resolve(result.data); // Return the parsed data
        },
        error: (error) => {
          reject(`Error parsing CSV: ${error.message}`);
        },
      });*/
    });
  });
}

// Example usage
const filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\complex_artists_examples.csv';

parseCSVFileFromPath(filePath)
  .then((data) => {
    console.log('Parsed CSV data:', data);
  })
  .catch((error) => {
    console.error(error);
  });
