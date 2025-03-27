"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseCSVFileFromPath = parseCSVFileFromPath;
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
                            // Step 1: Remove square brackets
                            var valueWithoutBrackets = value.replace(/[\[\]]/g, '');
                            // Step 2: Escape commas inside quoted strings by replacing with a placeholder
                            var valueWithEscapedCommas = valueWithoutBrackets.replace(/"([^"]*)"/g, function (match) {
                                return match.replace(/,/g, '\\comma\\');
                            });
                            // Step 3: Match all quoted strings and non-quoted parts, split by commas outside quotes
                            var artistsArray = valueWithEscapedCommas.match(/'([^']|\\')*'|"([^"]|\\")*"|[^,]+/g)
                                .map(function (item) { return item.trim().replace(/^['"]|['"]$/g, ''); }); // Remove surrounding quotes and trim spaces
                            // Step 4: Revert the escaped commas back to real commas
                            artistsArray = artistsArray.map(function (item) { return item.replace(/\\comma\\/g, ','); });
                            /*
                            console.log("Original value: <", JSON.stringify(value), ">");
                            console.log("Escaped value: <", JSON.stringify(valueWithEscapedCommas), ">");
                            console.log("Array value: <", JSON.stringify(artistsArray), ">");
                            */
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
var filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\some_tracks.csv';
// complex_artists_examples.csv';
parseCSVFileFromPath(filePath)
    .then(function (data) {
    // console.log('Parsed CSV data:', data);
    // console.log(data[0].artists[0]);
    console.log("Row count of tracks: ", data.length);
    var artistsWithTracks = new Set(data.flatMap(function (track) { return track.artists; }));
    console.log("Row count of artists values: ", artistsWithTracks.values.length);
    console.log("Row count of artists entries: ", artistsWithTracks.entries.length);
    console.log("Row count of artists entries: ", artistsWithTracks.size);
    console.log(artistsWithTracks);
})
    .catch(function (error) {
    console.error(error);
});
