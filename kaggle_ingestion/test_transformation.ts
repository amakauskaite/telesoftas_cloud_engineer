import * as tc from './parse_csv_from_file';

interface DynamicJson {
  [key: string]: any;
}

// Assign undefined if the month and/or day is missing
function parseDateParts(dateParts: string[]): { year: number | null; month: number | null; day: number | null } {
  const [year, month = null, day = null] = dateParts.map(part => (part ? parseInt(part, 10) : null));
  return { year, month, day };
}

// Generic function to explode a date field into year, month, and day
function explodeDateField(updatedJson: DynamicJson, dateField: string) {
  if (updatedJson[dateField] != null) {
    // Explicitly casting to string for cases when there's only the year known
    const dateParts = String(updatedJson[dateField]).split('-'); // Always convert to string and split
    const { year, month, day } = parseDateParts(dateParts);
    
    updatedJson['year'] = year;
    updatedJson['month'] = month;
    updatedJson['day'] = day;
  }
}

// Update danceability value
function stringifyDanceability(updatedJson: DynamicJson) {
  if (updatedJson['danceability'] >= 0 && updatedJson['danceability'] < 0.5) {
    updatedJson['danceability'] = 'Low';
  } else if (updatedJson['danceability'] >= 0.5 && updatedJson['danceability'] <= 0.6) {
    updatedJson['danceability'] = 'Medium';
  } else if (updatedJson['danceability'] > 0.6 && updatedJson['danceability'] <= 1) {
    updatedJson['danceability'] = 'High';
  } else {
    updatedJson['danceability'] = 'Undefined';
  }
}

// Main function to process JSON data
async function main() {
  const filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\some_tracks.csv';

  const data = await tc.parseCSVFileFromPath(filePath);
  console.log('Parsed first item:', data[0]);
  console.log('Parsed item with not full date:', data[4]);

  // Process each item individually
  const updatedJson = data.map((item) => {
    const updatedItem: DynamicJson = { ...item };

    explodeDateField(updatedItem, 'release_date');  // Explode release date
    stringifyDanceability(updatedItem);

    return updatedItem;
  });

  console.log(updatedJson);
}

// Run the main function
main();
