import * as tc from './test_conversion';

interface DynamicJson {
    [key: string]: any; // Allows any key with any value
}

// Assign undefined if the month and/or day is missing
function assignDateValues(dateParts: string[], updatedJson: DynamicJson) {
    updatedJson['year'] = parseInt(dateParts[0], 10);
    updatedJson['month'] = dateParts[1] ? parseInt(dateParts[1], 10) : null;
    updatedJson['day'] = dateParts[2] ? parseInt(dateParts[2], 10) : null;
}

function explodeDateFieldsInJson(json: DynamicJson[], dateFieldName: string): DynamicJson[] {
    return json.map((item) => {
        const updatedItem: DynamicJson = { ...item }; // Create a shallow copy to avoid mutating the original object

        const dateField = item[dateFieldName]; // Get the release date from the current object
        // Check if the release date exists and is not undefined or null
        if (dateField != null) {
            let dateParts: string[];

            // If the release date is a string, split it by '-'
            if (typeof dateField === 'string') {
                dateParts = dateField.split('-');
            }
            // If the release date is a number (just the year), handle it accordingly
            else if (typeof dateField === 'number') {
                dateParts = [dateField.toString()]; // Treat it as just a year
            } else {
                dateParts = []; // If the release date is neither string nor number, we can't process it
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



