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
    updatedJson['danceability'] = 'Undefined';
  } else if (danceability >= 0 && danceability < 0.5) {
    updatedJson['danceability'] = 'Low';
  } else if (danceability >= 0.5 && danceability <= 0.6) {
    updatedJson['danceability'] = 'Medium';
  } else if (danceability > 0.6 && danceability <= 1) {
    updatedJson['danceability'] = 'High';
  } else {
    updatedJson['danceability'] = 'Undefined';
  }
}

export function transformCSVField(value: string, field: string): any {
  // Return an empty array if value is falsy (null, undefined, or empty string)
  if (!value) {
    return [];
  }

  if (field === 'artists') {
    const cleanedValue = value.replace(/^\[|\]$/g, ''); // Remove square brackets around the array

    // Match quoted or unquoted values correctly, including handling escaped quotes and commas inside quotes
    const matches = cleanedValue.match(/"([^"\\]*(\\.[^"\\]*)*)"|'([^'\\]*(\\.[^'\\]*)*)'|[^,]+/g);

    return matches
      ? matches.map(item => item.trim().replace(/^['"]|['"]$/g, '').replace(/\\"/g, '"'))
      : [];
  }
  return value;  // If the field is not 'artists', return the original value
}
