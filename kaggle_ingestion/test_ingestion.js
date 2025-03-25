"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var axios_1 = require("axios");
var AWS = require("aws-sdk");
var Papa = require("papaparse");
var JSZip = require("jszip");
var BUCKET_NAME = 'auma-spotify';
var ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
var TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
var ARTISTS_FILENAME = 'artists.csv';
var TRACKS_FILENAME = 'tracks.csv';
// Initialize AWS S3
// Using an IAM user with AWS Toolkit to store credentials, so they're not being passed here in the constructor
var s3 = new AWS.S3();
// Helper function to download and parse CSV from URL
function downloadCSV(url) {
    return __awaiter(this, void 0, void 0, function () {
        var response;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, axios_1.default.get(url)];
                case 1:
                    response = _a.sent();
                    return [2 /*return*/, new Promise(function (resolve, reject) {
                            Papa.parse(response.data, {
                                header: true,
                                dynamicTyping: true,
                                complete: function (results) { return resolve(results.data); },
                                error: function (err) { return reject(err); },
                            });
                        })];
            }
        });
    });
}
// Helper function to download and parse CSV from a ZIP file
function downloadAndExtractCSV(url) {
    return __awaiter(this, void 0, void 0, function () {
        var response, zip, csvFileName, csvFile, csvData;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, axios_1.default.get(url, { responseType: 'arraybuffer' })];
                case 1:
                    response = _a.sent();
                    return [4 /*yield*/, JSZip.loadAsync(response.data)];
                case 2:
                    zip = _a.sent();
                    csvFileName = Object.keys(zip.files).find(function (fileName) { return fileName.endsWith('.csv'); });
                    if (!csvFileName) {
                        throw new Error('No CSV file found in the ZIP archive.');
                    }
                    csvFile = zip.files[csvFileName];
                    return [4 /*yield*/, csvFile.async('text')];
                case 3:
                    csvData = _a.sent();
                    // Parse the CSV content using PapaParse
                    return [2 /*return*/, new Promise(function (resolve, reject) {
                            Papa.parse(csvData, {
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
                        })];
            }
        });
    });
}
// Filter tracks file
// Should only have rows where there's a track name AND the tracks is longer than 1 minute (or 1 minute long)
function filterTracks(data) {
    console.log("Null row cnt:", data.filter(function (row) { return row.name === null; }).length, "short song row cnt:", data.filter(function (row) { return row.duration_ms < 60000; }).length);
    return data.filter(function (row) { return row.name !== null && row.duration_ms >= 60000; });
}
// Filter artists file to only have artists with tracks in the filtered tracks file
function filterArtists(artists, artistsFromTracks) {
    // console.log(data[0])
    // console.log("First 5 artists in tracks:", artists[0].name, ", ",artists[1].name,", ", artists[2].name,", ",artists[3].name,", ",artists[4].name)
    // console.log("First 5 artists in artists:", artistsFromTracks[0], ", ",artistsFromTracks[1],", ", artistsFromTracks[2],", ",artistsFromTracks[3],", ",artistsFromTracks[4])
    // For each artist in artists file check if if the artist's name is in the list of artistsFromTracks
    try {
        return artists.filter(function (artist) { return artistsFromTracks.has(artist.name); });
    }
    catch (error) {
        console.error('Error:', error);
        return artists;
    }
}
// Upload CSV file to S3
function uploadCSVToS3(fileName, fileContent, bucketName) {
    return __awaiter(this, void 0, void 0, function () {
        var params, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    params = {
                        Bucket: bucketName,
                        Key: fileName,
                        Body: fileContent,
                        ContentType: 'text/csv',
                    };
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.upload(params).promise()];
                case 2:
                    _a.sent();
                    console.log("Successfully uploaded ".concat(fileName, " to S3"));
                    return [3 /*break*/, 4];
                case 3:
                    error_1 = _a.sent();
                    console.error('Error uploading file to S3:', error_1);
                    return [3 /*break*/, 4];
                case 4: return [2 /*return*/];
            }
        });
    });
}
// TODO: move helper functions to a separate file. Make them reusable, if possible
// Main function to download, filter, and upload the CSV files
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var tracks, artists, filteredTracks, artistsWithTracks, filteredArtists, error_2;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 3, , 4]);
                    return [4 /*yield*/, downloadAndExtractCSV(TRACKS_URL)];
                case 1:
                    tracks = _a.sent();
                    console.log('1. File downloaded');
                    console.log('Row count:', tracks.length);
                    return [4 /*yield*/, downloadAndExtractCSV(ARTISTS_URL)];
                case 2:
                    artists = _a.sent();
                    console.log('2. File downloaded');
                    console.log('Row count:', tracks.length);
                    filteredTracks = filterTracks(tracks);
                    console.log('3. File filtered');
                    console.log('Row count:', filteredTracks.length);
                    console.log(filteredTracks[0].artists);
                    console.log(filteredTracks[827].artists);
                    artistsWithTracks = new Set(filteredTracks.flatMap(function (track) { return track.artists; }));
                    console.log('4. Artists with tracks taken');
                    console.log('Row count:', artistsWithTracks.values.length);
                    filteredArtists = filterArtists(artists, artistsWithTracks);
                    console.log('5. File filtered');
                    console.log('Row count:', filteredArtists.length);
                    console.log(filteredArtists[0]);
                    return [3 /*break*/, 4];
                case 3:
                    error_2 = _a.sent();
                    console.error('Error:', error_2);
                    return [3 /*break*/, 4];
                case 4: return [2 /*return*/];
            }
        });
    });
}
// Run the main function
main();
