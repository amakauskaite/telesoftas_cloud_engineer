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