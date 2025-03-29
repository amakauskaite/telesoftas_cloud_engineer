import * as tc from './parse_csv_from_file';
import * as Papa from 'papaparse';

// Example usage
const filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion_testing\\complex_artists_examples.csv';
// some_tracks.csv';

function addQuotesToBrackets(data: any[], columnName: string): any[] {
  return data.map(row => {
      if (row[columnName] && typeof row[columnName] === 'string') {
          row[columnName] = row[columnName].replace(/^\[/, '"[').replace(/\]$/, ']"');
      }
      return row;
  });
}

tc.parseCSVFileFromPath(filePath)
  .then((data) => {
    console.log('Parsed CSV data:', data);
    // console.log(data[0].artists[0]);

    // console.log("Row count of tracks: ", data.length)

    const unparsed = Papa.unparse(addQuotesToBrackets(data, 'artists'), {
      // quotes: true,  // Ensure proper escaping of quotes and commas
      delimiter: ',',
      newline: '\n',
      header: true,
    });

    console.log('Unparsed data:', unparsed);

    /*
    const artistsWithTracks = new Set(data.flatMap(track => track.artists))
    console.log("Row count of artists values: ", artistsWithTracks.values.length)
    console.log("Row count of artists entries: ", artistsWithTracks.entries.length)
    console.log("Row count of artists entries: ", artistsWithTracks.size)
    console.log(artistsWithTracks)
    */
  })
  .catch((error) => {
    console.error(error);
  });

