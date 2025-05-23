// Filter tracks to only include valid tracks (with a name and duration >= 60 seconds)
export function filterTracks(data: any[]): any[] {
  return data.filter((row) => row.name !== null && row.duration_ms >= 60000);
}

// Filter artists to only include those who have tracks in the filtered tracks list
export function filterArtists(artists: any[], artistsFromTracks: Set<string>): any[] {
  return artists.filter((artist) => artistsFromTracks.has(artist.name));
}

// Create year, month, day fields; assign undefined if the month and/or day is missing
export function parseDateParts(dateParts: string[]): { year: number | null; month: number | null; day: number | null } {
  const [year, month = null, day = null] = dateParts.map(part => (part ? parseInt(part, 10) : null));
  return { year, month, day };
}

// Generic function to explode a date field into year, month, and day
export function explodeDateField(updatedJson: any, dateField: string) {
  if (updatedJson[dateField] != null) {
    // Explicitly casting to string for cases when there's only the year known
    const dateParts = String(updatedJson[dateField]).split('-');
    const { year, month, day } = parseDateParts(dateParts);

    updatedJson['year'] = year;
    updatedJson['month'] = month;
    updatedJson['day'] = day;
  }
}

// Update danceability value
export function stringifyDanceability(updatedJson: any) {

  const danceability = updatedJson['danceability'];

  if (danceability === null || danceability === undefined || isNaN(danceability)) {
    updatedJson['danceability'] = null;
  } else if (danceability >= 0 && danceability < 0.5) {
    updatedJson['danceability'] = 'Low';
  } else if (danceability >= 0.5 && danceability <= 0.6) {
    updatedJson['danceability'] = 'Medium';
  } else if (danceability > 0.6 && danceability <= 1) {
    updatedJson['danceability'] = 'High';
  } else {
    updatedJson['danceability'] = null;
  }
}

export function transformCSVField(value: string, field: string): any {
  // Return an empty array if value is falsy (null, undefined, or empty string)
  if (!value) {
    return [];
  }

  if (field === 'artists') {
    // Cleaning up the artists field that will be used later
    // We'll need an array of strings not just a string with an array inside
    const cleanedValue = value
      // Remove outer square brackets
      .replace(/[\[\]]/g, '')
      // Change comma's between single/double qoutes with dummy value
      .replace(/(["'])(.*?)(\1)/g, (match, quote, content) => 
        `${quote}${content.replace(/,/g, '\\comma\\')}${quote}`);

    // Split by unescaped single/double qoutes and commas
    const matches = cleanedValue.match(/'([^']|\\')*'|"([^"]|\\")*"|[^,]+/g)
    
    return matches ?
      matches
        // Remove duplicate qoutes
        .map(item => item.trim().replace(/^['"]|['"]$/g, ''))
        // Change dummy value to actual comma
        .map(item => item.replace(/\\comma\\/g, ','))
      : [];
  }
  return value;  // If the field is not 'artists', return the original value
}
