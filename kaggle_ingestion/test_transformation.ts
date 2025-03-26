import * as tc from './test_conversion';

interface DynamicJson {
    [key: string]: any; // Allows any key with any value
}

// Assign undefined if the month and/or day is missing
function assignDateValues(dateParts: string[], updatedJson: DynamicJson) {
    const [year, month = null, day = null] = dateParts.map(part => part ? parseInt(part, 10) : null);
    
    updatedJson['year'] = year;
    updatedJson['month'] = month;
    updatedJson['day'] = day;
  }
  
  // Explode the date field into separate year, month, and day fields
  function explodeDateFieldsInJson(json: DynamicJson[], dateFieldName: string): DynamicJson[] {
    return json.map((item) => {
      const updatedItem: DynamicJson = { ...item }; // Shallow copy to avoid mutating the original item
  
      const dateField = item[dateFieldName]; // Get the release date from the current object
  
      if (dateField != null) {
        let dateParts: string[];
  
        // Handle the dateField based on its type
        if (typeof dateField === 'string') {
          dateParts = dateField.split('-');
        } else if (typeof dateField === 'number') {
          dateParts = [dateField.toString()]; // Treat it as just a year
        } else {
          dateParts = []; // Invalid format, handle as empty
        }
  
        // Call the function to assign values to the updatedItem
        assignDateValues(dateParts, updatedItem);
      }
  
      return updatedItem; // Return the modified item
    });
  }  

const filePath = 'C:\\Users\\ausri\\OneDrive\\Documents\\GitHub\\telesoftas_cloud_engineer\\kaggle_ingestion\\some_tracks.csv';

tc.parseCSVFileFromPath(filePath).then((data) => {
    console.log('Parsed first item:', data[0]);
    console.log('Parsed item with not full date:', data[4]);

    const updatedJson = explodeDateFieldsInJson(data, 'release_date');
    console.log(updatedJson)

})



