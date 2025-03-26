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
    var _a = dateParts.map(function (part) { return part ? parseInt(part, 10) : null; }), year = _a[0], _b = _a[1], month = _b === void 0 ? null : _b, _c = _a[2], day = _c === void 0 ? null : _c;
    updatedJson['year'] = year;
    updatedJson['month'] = month;
    updatedJson['day'] = day;
}
// Explode the date field into separate year, month, and day fields
function explodeDateFieldsInJson(json, dateFieldName) {
    return json.map(function (item) {
        var updatedItem = __assign({}, item); // Shallow copy to avoid mutating the original item
        var dateField = item[dateFieldName]; // Get the release date from the current object
        if (dateField != null) {
            var dateParts = void 0;
            // Handle the dateField based on its type
            if (typeof dateField === 'string') {
                dateParts = dateField.split('-');
            }
            else if (typeof dateField === 'number') {
                dateParts = [dateField.toString()]; // Treat it as just a year
            }
            else {
                dateParts = []; // Invalid format, handle as empty
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
