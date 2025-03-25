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
              // Clean value by removing square brackets
              const cleanedValue = value.replace(/[\[\]]/g, '');

              // Match all quoted strings and non-quoted parts, split by commas outside quotes
              let artistsArray = cleanedValue.match(/'[^']*'|"[^"]*"|[^,]+/g)
                .map(item => item.trim().replace(/^['"]|['"]$/g, '')); // Remove surrounding quotes and trim spaces

              console.log("Original value: <", JSON.stringify(value), ">");
              console.log("Cleaned value: <", JSON.stringify(cleanedValue), ">");
              console.log("Array value: <", JSON.stringify(artistsArray), ">");

              return artistsArray; // Return as an array
            } catch (e) {
              console.error('Parsing error for value:', JSON.stringify(value), e);
              return value; // If it fails, return the original value (can handle cases where it's not a valid array string)
            }
          }
          return value; // Leave other fields unchanged
        },
        complete: (result) => {
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
  })
  .catch((error) => {
    console.error(error);
  });
