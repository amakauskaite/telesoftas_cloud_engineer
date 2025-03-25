"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var fs = require("fs");
var Papa = require("papaparse");
function parseCSVFileFromPath(filePath) {
    return new Promise(function (resolve, reject) {
        // Read the file from the local filesystem
        fs.readFile(filePath, 'utf8', function (err, data) {
            if (err) {
                reject("Error reading file: ".concat(err));
                return;
            }
            Papa.parse(data, {
                header: true,
                dynamicTyping: true, // Automatically convert values like numbers
                transform: function (value, field) {
                    if (field === 'artists') {
                        try {
                            return JSON.parse(value); // Converts stringified arrays into actual arrays
                        }
                        catch (e) {
                            return value; // If it fails, return the original value (can handle cases where it's not a valid array string)
                        }
                    }
                    return value; // Leave other fields unchanged
                },
                complete: function (result) {
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
var filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\complex_artists_examples.csv';
parseCSVFileFromPath(filePath)
    .then(function (data) {
    console.log('Parsed CSV data:', data);
})
    .catch(function (error) {
    console.error(error);
});
