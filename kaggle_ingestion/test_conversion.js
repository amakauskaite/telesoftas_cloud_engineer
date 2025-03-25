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
                            // Clean value by removing square brackets
                            var cleanedValue = value.replace(/[\[\]]/g, '');
                            // Match all quoted strings and non-quoted parts, split by commas outside quotes
                            var artistsArray = cleanedValue.match(/'[^']*'|"[^"]*"|[^,]+/g)
                                .map(function (item) { return item.trim().replace(/^['"]|['"]$/g, ''); }); // Remove surrounding quotes and trim spaces
                            console.log("Original value: <", JSON.stringify(value), ">");
                            console.log("Cleaned value: <", JSON.stringify(cleanedValue), ">");
                            console.log("Array value: <", JSON.stringify(artistsArray), ">");
                            return artistsArray; // Return as an array
                        }
                        catch (e) {
                            console.error('Parsing error for value:', JSON.stringify(value), e);
                            return value; // If it fails, return the original value (can handle cases where it's not a valid array string)
                        }
                    }
                    return value; // Leave other fields unchanged
                },
                complete: function (result) {
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
})
    .catch(function (error) {
    console.error(error);
});
