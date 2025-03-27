"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var kaggle_node_1 = require("kaggle-node");
var kaggleNode = new kaggle_node_1.KaggleNode({
    credentials: {
        username: 'makauskaite',
        key: 'e2cac6f45e810bab9463419b061e3bd7'
    }
});
// let res = await kaggleNode.datasets.search();
var handleStr = 'yamaerenay/spotify-dataset-19212020-600k-tracks';
var res = kaggleNode.datasets.list(handleStr); // application/json
console.log(res.toString());
