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
var tc = require("./parse_csv_from_file");
// Assign undefined if the month and/or day is missing
function parseDateParts(dateParts) {
    var _a = dateParts.map(function (part) { return (part ? parseInt(part, 10) : null); }), year = _a[0], _b = _a[1], month = _b === void 0 ? null : _b, _c = _a[2], day = _c === void 0 ? null : _c;
    return { year: year, month: month, day: day };
}
// Generic function to explode a date field into year, month, and day
function explodeDateField(updatedJson, dateField) {
    if (updatedJson[dateField] != null) {
        // Explicitly casting to string for cases when there's only the year known
        var dateParts = String(updatedJson[dateField]).split('-'); // Always convert to string and split
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
// Main function to process JSON data
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var filePath, data, updatedJson;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\some_tracks.csv';
                    return [4 /*yield*/, tc.parseCSVFileFromPath(filePath)];
                case 1:
                    data = _a.sent();
                    console.log('Parsed first item:', data[0]);
                    console.log('Parsed item with not full date:', data[4]);
                    updatedJson = data.map(function (item) {
                        var updatedItem = __assign({}, item);
                        explodeDateField(updatedItem, 'release_date'); // Explode release date
                        // explodeDateField(updatedItem, 'recorded_date'); // If there's another date field, process it too
                        stringifyDanceability(updatedItem);
                        return updatedItem;
                    });
                    console.log(updatedJson);
                    return [2 /*return*/];
            }
        });
    });
}
// Run the main function
main();
