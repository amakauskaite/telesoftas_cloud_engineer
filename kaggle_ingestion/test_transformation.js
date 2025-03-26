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
Object.defineProperty(exports, "__esModule", { value: true });
var tc = require("./test_conversion");
// Assign undefined if the month and/or day is missing
function assignDateValues(dateParts, updatedJson) {
    updatedJson['year'] = parseInt(dateParts[0], 10);
    updatedJson['month'] = dateParts[1] ? parseInt(dateParts[1], 10) : undefined;
    updatedJson['day'] = dateParts[2] ? parseInt(dateParts[2], 10) : undefined;
}
function explodeDateFieldsInJson(json, dateFieldName) {
    return json.map(function (item) {
        var updatedItem = __assign({}, item); // Create a shallow copy to avoid mutating the original object
        var dateField = item[dateFieldName]; // Get the release date from the current object
        // Check if the release date exists and is not undefined or null
        if (dateField != null) {
            var dateParts = void 0;
            // If the release date is a string, split it by '-'
            if (typeof dateField === 'string') {
                dateParts = dateField.split('-');
            }
            // If the release date is a number (just the year), handle it accordingly
            else if (typeof dateField === 'number') {
                dateParts = [dateField.toString()]; // Treat it as just a year
            }
            else {
                dateParts = []; // If the release date is neither string nor number, we can't process it
            }
            // Call the function to assign values to the updatedItem
            assignDateValues(dateParts, updatedItem);
        }
        return updatedItem; // Return the modified item
    });
}
var filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\some_tracks.csv';
tc.parseCSVFileFromPath(filePath).then(function (data) {
    console.log('Parsed first item:', data[0]);
    console.log('Parsed item with not full date:', data[4]);
    var updatedJson = explodeDateFieldsInJson(data, 'release_date');
    console.log(updatedJson);
});
