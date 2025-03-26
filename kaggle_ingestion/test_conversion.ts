import * as tc from './parse_csv_from_file';

// Example usage
const filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\some_tracks.csv';
// complex_artists_examples.csv';

tc.parseCSVFileFromPath(filePath)
  .then((data) => {
    // console.log('Parsed CSV data:', data);
    // console.log(data[0].artists[0]);

    console.log("Row count of tracks: ", data.length)

    const artistsWithTracks = new Set(data.flatMap(track => track.artists))
    console.log("Row count of artists values: ", artistsWithTracks.values.length)
    console.log("Row count of artists entries: ", artistsWithTracks.entries.length)
    console.log("Row count of artists entries: ", artistsWithTracks.size)
    console.log(artistsWithTracks)
  })
  .catch((error) => {
    console.error(error);
  });

