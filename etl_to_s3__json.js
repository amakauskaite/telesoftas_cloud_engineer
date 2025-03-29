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
var client_s3_1 = require("@aws-sdk/client-s3");
var file = require("./file_handling");
var BUCKET_NAME = 'auma-spotify';
var ARTISTS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/artists.csv';
var TRACKS_URL = 'https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks/tracks.csv';
var ARTISTS_FILENAME = 'artists.json';
var TRACKS_FILENAME = 'tracks.json';
// Filter tracks to only include valid tracks (with a name and duration >= 60 seconds)
function filterTracks(data) {
    return data.filter(function (row) { return row.name !== null && row.duration_ms >= 60000; });
}
// Filter artists to only include those who have tracks in the filtered tracks list
function filterArtists(artists, artistsFromTracks) {
    return artists.filter(function (artist) { return artistsFromTracks.has(artist.name); });
}
// Create year, month, day fields; assign undefined if the month and/or day is missing
function parseDateParts(dateParts) {
    var _a = dateParts.map(function (part) { return (part ? parseInt(part, 10) : null); }), year = _a[0], _b = _a[1], month = _b === void 0 ? null : _b, _c = _a[2], day = _c === void 0 ? null : _c;
    return { year: year, month: month, day: day };
}
// Generic function to explode a date field into year, month, and day
function explodeDateField(updatedJson, dateField) {
    if (updatedJson[dateField] != null) {
        // Explicitly casting to string for cases when there's only the year known
        var dateParts = String(updatedJson[dateField]).split('-');
        var _a = parseDateParts(dateParts), year = _a.year, month = _a.month, day = _a.day;
        updatedJson['year'] = year;
        updatedJson['month'] = month;
        updatedJson['day'] = day;
    }
}
// Update danceability value
function stringifyDanceability(updatedJson) {
    if (updatedJson['danceability'] >= 0 && updatedJson['danceability'] < 0.5) {
        updatedJson['danceability'] = 'Low';
    }
    else if (updatedJson['danceability'] >= 0.5 && updatedJson['danceability'] <= 0.6) {
        updatedJson['danceability'] = 'Medium';
    }
    else if (updatedJson['danceability'] > 0.6 && updatedJson['danceability'] <= 1) {
        updatedJson['danceability'] = 'High';
    }
    else {
        updatedJson['danceability'] = 'Undefined';
    }
}
// First function of the main flow - processing tracks.csv
// Returns a set of artists that have tracks in the filtered file
function processTracks(s3) {
    return __awaiter(this, void 0, void 0, function () {
        var tracks, filteredTracks, artistsSet;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    console.log('Downloading tracks CSV...');
                    return [4 /*yield*/, file.downloadAndExtractCSV(TRACKS_URL)];
                case 1:
                    tracks = _a.sent();
                    console.log('Filtering tracks...');
                    filteredTracks = filterTracks(tracks);
                    console.log('Processing release dates and danceability...');
                    filteredTracks = filteredTracks.map(function (item) {
                        var updatedItem = __assign({}, item);
                        explodeDateField(updatedItem, 'release_date'); // Explode release date
                        stringifyDanceability(updatedItem);
                        return updatedItem;
                    });
                    console.log('Uploading tracks to S3...');
                    return [4 /*yield*/, file.uploadJSONToS3(TRACKS_FILENAME, filteredTracks, BUCKET_NAME, s3)];
                case 2:
                    _a.sent();
                    artistsSet = new Set(filteredTracks.flatMap(function (track) { return track.artists; }));
                    // Free memory as we're not going to use tracks data anymore
                    console.log('Releasing memory...');
                    filteredTracks.length = 0;
                    tracks.length = 0;
                    return [2 /*return*/, artistsSet];
            }
        });
    });
}
// Second function of the main flow - processing artists.csv
// Using the set of artists from tracks
function processArtists(artistsInTracks, s3) {
    return __awaiter(this, void 0, void 0, function () {
        var artists, filteredArtists;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, file.downloadAndExtractCSV(ARTISTS_URL)];
                case 1:
                    artists = _a.sent();
                    console.log('CSV file downloaded');
                    filteredArtists = filterArtists(artists, artistsInTracks);
                    console.log('Artists filtered');
                    // Upload the filtered tracks file to AWS S3
                    return [4 /*yield*/, file.uploadJSONToS3(ARTISTS_FILENAME, filteredArtists, BUCKET_NAME, s3)];
                case 2:
                    // Upload the filtered tracks file to AWS S3
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var s3, artistsInTracks, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    s3 = new client_s3_1.S3Client({
                        region: 'eu-north-1',
                        // Credentials for testing, will be removed later to not be abused
                        credentials: {
                            accessKeyId: 'AKIAX5T2WSIPGYLZQKQO',
                            secretAccessKey: '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',
                        },
                    });
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 4, , 5]);
                    return [4 /*yield*/, processTracks(s3)];
                case 2:
                    artistsInTracks = _a.sent();
                    // Process artists after tracks are cleared
                    return [4 /*yield*/, processArtists(artistsInTracks, s3)];
                case 3:
                    // Process artists after tracks are cleared
                    _a.sent();
                    return [3 /*break*/, 5];
                case 4:
                    error_1 = _a.sent();
                    console.error('Error:', error_1);
                    return [3 /*break*/, 5];
                case 5: return [2 /*return*/];
            }
        });
    });
}
// Run the main function
main();
