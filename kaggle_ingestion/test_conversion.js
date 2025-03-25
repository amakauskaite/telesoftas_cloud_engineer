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
                            var cleanedValue = JSON.parse(JSON.stringify(value.replace(/[\[\]]/g, '')));
                            var artistsArray = cleanedValue.match(/'[^']+'|"[^"]+"|[^,]+/g).map(function (item) { return item.trim().replace(/^['"]|['"]$/g, ''); });
                            // .replace(/^.(.*).$/, '$1')
                            // .replace(/\\"/g, '\\\\"')
                            // .replace(/'([^']+)'/g, '"$1"')
                            //;   // Replace single quotes around array elements with double quotes
                            // Now, safely parse the value
                            console.log("Original value: <", JSON.stringify(value), ">");
                            console.log("Cleaned value: <", JSON.stringify(value), ">");
                            console.log("Array value: <", JSON.stringify(artistsArray), ">");
                            // console.log("Cleaned value interim: <",JSON.stringify(cleanedValue0),">")
                            // console.log("Cleaned value final: <",JSON.stringify(value.replace(/[\[\]]/g,'')),">")
                            return artistsArray; // Converts stringified arrays into actual arrays
                        }
                        catch (e) {
                            console.error('Parsing error for value:', JSON.stringify(value), e);
                            return value; // If it fails, return the original value (can handle cases where it's not a valid array string)
                        }
                    }
                    return value; // Leave other fields unchanged
                },
                complete: function (result) {
                    // console.log(result.data); // Your parsed CSV data
                    resolve(result.data); // Resolving the promise with parsed data
                },
                error: function (error) {
                    reject("Error parsing CSV: ".concat(error.message));
                },
            });
        });
    });
}
// Example usage
var filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\complex_artists_examples.csv';
parseCSVFileFromPath(filePath)
    .then(function (data) {
    console.log('Parsed CSV data:', data);
    console.log(data[0].artists[0]);
})
    .catch(function (error) {
    console.error(error);
});
