import { KaggleNode } from 'kaggle-node';

let kaggleNode = new KaggleNode({
  credentials: {
      username: 'makauskaite',
      key: 'e2cac6f45e810bab9463419b061e3bd7'
  }
});

// let res = await kaggleNode.datasets.search();

let handleStr = 'yamaerenay/spotify-dataset-19212020-600k-tracks';

let res = kaggleNode.datasets.list(handleStr); // application/json

console.log(res.toString());