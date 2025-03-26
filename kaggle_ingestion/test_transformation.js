"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var tc = require("./test_conversion");
var filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\some_tracks.csv';
tc.parseCSVFileFromPath(filePath).then(function (data) {
    console.log('Parsed first item:', data[0]);
    console.log('Parsed item with not full date:', data[4]);
    // console.log(data[0].artists[0]);
});
