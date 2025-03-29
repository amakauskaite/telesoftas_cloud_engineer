"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var tc = require("./parse_csv_from_file");
var Papa = require("papaparse");
// Example usage
var filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion_testing\\complex_artists_examples.csv';
// some_tracks.csv';
function addQuotesToBrackets(data, columnName) {
    return data.map(function (row) {
        if (row[columnName] && typeof row[columnName] === 'string') {
            row[columnName] = row[columnName].replace(/^\[/, '"[').replace(/\]$/, ']"');
        }
        return row;
    });
}
tc.parseCSVFileFromPath(filePath)
    .then(function (data) {
    console.log('Parsed CSV data:', data);
    // console.log(data[0].artists[0]);
    // console.log("Row count of tracks: ", data.length)
    var unparsed = Papa.unparse(addQuotesToBrackets(data, 'artists'), {
        // quotes: true,  // Ensure proper escaping of quotes and commas
        delimiter: ',',
        newline: '\n',
        header: true,
    });
    console.log('Unparsed data:', unparsed);
    /*
    const artistsWithTracks = new Set(data.flatMap(track => track.artists))
    console.log("Row count of artists values: ", artistsWithTracks.values.length)
    console.log("Row count of artists entries: ", artistsWithTracks.entries.length)
    console.log("Row count of artists entries: ", artistsWithTracks.size)
    console.log(artistsWithTracks)
    */
})
    .catch(function (error) {
    console.error(error);
});
