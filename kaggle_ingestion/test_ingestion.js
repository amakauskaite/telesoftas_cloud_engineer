"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
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
var client_s3_1 = require("@aws-sdk/client-s3"); // Import S3Client and PutObjectCommand from AWS SDK v3
var Papa = require("papaparse");
var JSZip = require("jszip");
var BUCKET_NAME = 'auma-spotify';
var ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
var TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
var ARTISTS_FILENAME = 'artists.csv';
var TRACKS_FILENAME = 'tracks.csv';
// Initialize AWS S3 Client (v3)
var s3 = new client_s3_1.S3Client({
    region: 'eu-north-1', // Replace with the appropriate region
});
// Helper function to download and parse CSV from URL
function downloadAndParseCSV(url) {
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
                                complete: function (result) { return resolve(result.data); },
                                error: function (error) { return reject("Error parsing CSV: ".concat(error.message)); },
                            });
                        })];
            }
        });
    });
}
// Helper function to download and extract CSV from ZIP file
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
                    return [2 /*return*/, new Promise(function (resolve, reject) {
                            Papa.parse(csvData, {
                                header: true,
                                dynamicTyping: true,
                                transform: function (value, field) {
                                    if (field === 'artists') {
                                        var cleanedValue = value.replace(/[\[\]]/g, '')
                                            .replace(/"([^"]*)"/g, function (match) { return match.replace(/,/g, '\\comma\\'); });
                                        var artistsArray = cleanedValue.match(/'([^']|\\')*'|"([^"]|\\")*"|[^,]+/g)
                                            .map(function (item) { return item.trim().replace(/^['"]|['"]$/g, ''); })
                                            .map(function (item) { return item.replace(/\\comma\\/g, ','); });
                                        return artistsArray;
                                    }
                                    return value;
                                },
                                complete: function (result) { return resolve(result.data); },
                                error: function (error) { return reject("Error parsing CSV: ".concat(error.message)); },
                            });
                        })];
            }
        });
    });
}
// Filter tracks to only include valid tracks (with name and duration >= 60 seconds)
function filterTracks(data) {
    return data.filter(function (row) { return row.name !== null && row.duration_ms >= 60000; });
}
// Filter artists to only include those who have tracks in the filtered tracks list
function filterArtists(artists, artistsFromTracks) {
    return artists.filter(function (artist) { return artistsFromTracks.has(artist.name); });
}
// Helper function to assign year, month, and day from date string
function assignDateValues(dateParts, updatedJson) {
    var _a = dateParts.map(function (part) { return part ? parseInt(part, 10) : null; }), year = _a[0], _b = _a[1], month = _b === void 0 ? null : _b, _c = _a[2], day = _c === void 0 ? null : _c;
    updatedJson['year'] = year;
    updatedJson['month'] = month;
    updatedJson['day'] = day;
}
// Explode the date field into separate year, month, and day fields
function explodeDateFieldsInJson(json, dateFieldName) {
    return json.map(function (item) {
        var updatedItem = __assign({}, item);
        var dateField = item[dateFieldName];
        if (dateField) {
            // Always convert the dateField to a string and split by '-'
            var dateParts = dateField.toString().split('-');
            // Assign the extracted values (year, month, day) to the updatedItem
            assignDateValues(dateParts, updatedItem);
        }
        else {
            // Handle cases where dateField is invalid or missing
            updatedItem['year'] = null;
            updatedItem['month'] = null;
            updatedItem['day'] = null;
        }
        return updatedItem;
    });
}
// Upload CSV file to S3 (v3)
function uploadCSVToS3(fileName, fileContent, bucketName) {
    return __awaiter(this, void 0, void 0, function () {
        var params, command, error_1;
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
                    command = new client_s3_1.PutObjectCommand(params);
                    return [4 /*yield*/, s3.send(command)];
                case 2:
                    _a.sent(); // Using the send() method to execute the command
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
// Main function to download, filter, and upload the CSV files
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var _a, tracks, artists, filteredTracks, artistsWithTracks, filteredArtists, error_2;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    _b.trys.push([0, 2, , 3]);
                    return [4 /*yield*/, Promise.all([
                            downloadAndExtractCSV(TRACKS_URL),
                            downloadAndExtractCSV(ARTISTS_URL),
                        ])];
                case 1:
                    _a = _b.sent(), tracks = _a[0], artists = _a[1];
                    console.log('CSV files downloaded');
                    filteredTracks = filterTracks(tracks);
                    console.log('Tracks filtered');
                    artistsWithTracks = new Set(filteredTracks.flatMap(function (track) { return track.artists; }));
                    filteredArtists = filterArtists(artists, artistsWithTracks);
                    console.log('Artists filtered');
                    // Explode the date fields in tracks
                    filteredTracks = explodeDateFieldsInJson(filteredTracks, 'release_date');
                    console.log('Date fields exploded in tracks');
                    return [3 /*break*/, 3];
                case 2:
                    error_2 = _b.sent();
                    console.error('Error:', error_2);
                    return [3 /*break*/, 3];
                case 3: return [2 /*return*/];
            }
        });
    });
}
// Run the main function
main();
