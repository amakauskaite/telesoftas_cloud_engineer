"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.filterTracks = filterTracks;
exports.filterArtists = filterArtists;
exports.parseDateParts = parseDateParts;
exports.explodeDateField = explodeDateField;
exports.stringifyDanceability = stringifyDanceability;
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
