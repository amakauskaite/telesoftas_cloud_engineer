import * as fs from 'fs';
import * as Papa from 'papaparse';
import * as trans from '../data_transformation/transformations';

export function parseCSVFileFromPath_v1(filePath: string): Promise<any[]> {
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
              // Step 1: Remove square brackets
              const valueWithoutBrackets = value.replace(/[\[\]]/g, '');

              // Step 2: Escape commas inside quoted strings by replacing with a placeholder
              const valueWithEscapedCommas = valueWithoutBrackets.replace(/"([^"]*)"/g, (match) => {
                return match.replace(/,/g, '\\comma\\');
              });

              // Step 3: Match all quoted strings and non-quoted parts, split by commas outside quotes
              let artistsArray = valueWithEscapedCommas.match(/'([^']|\\')*'|"([^"]|\\")*"|[^,]+/g)
                .map(item => item.trim().replace(/^['"]|['"]$/g, '')); // Remove surrounding quotes and trim spaces

              // Step 4: Revert the escaped commas back to real commas
              artistsArray = artistsArray.map(item => item.replace(/\\comma\\/g, ','));

              /*
              console.log("Original value: <", JSON.stringify(value), ">");
              console.log("Escaped value: <", JSON.stringify(valueWithEscapedCommas), ">");
              console.log("Array value: <", JSON.stringify(artistsArray), ">");
              */

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

export function parseCSVFileFromPath_v2(filePath: string): Promise<any[]> {
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
        transform: trans.transformCSVField,
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

export function parseCSVFileFromPath_v3(filePath: string): Promise<any[]> {
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
          if (!value) {
            return [];
          }
        
          if (field === 'artists') {

            const cleanedValue = value
            // Remove outer square brackets
            .replace(/^\[|\]$/g, '')
            // change comma's between single/double quotes with dummy value
            .replace(/(["'])(.*?)(\1)/g, (match, quote, content) => 
              `${quote}${content.replace(/,/g, '\\comma\\')}${quote}`)
            // split by unescaped single quotes, unescaped double quotes and commas
            .match(/'([^']|\\')*'|"([^"]|\\")*"|[^,]+/g)
            ;

            return cleanedValue ?
              cleanedValue.map(item => item.trim().replace(/^['"]|['"]$/g, '')).map(item => item.replace(/\\comma\\/g, ','))
              : [];
          }
          return value;  // If the field is not 'artists', return the original value
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

export function parseCSVFileFromPath_v4(filePath: string): Promise<any[]> {
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
          if (!value) {
            return [];
          }
        
          if (field === 'artists') {
            return value ?
              value.map(item => item.trim().replace(/^['"]|['"]$/g, ''))
              : [];
          }
          return value;  // If the field is not 'artists', return the original value
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

export function parseCSVFileFromPathWithoutTransformations(filePath: string): Promise<any[]> {
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
