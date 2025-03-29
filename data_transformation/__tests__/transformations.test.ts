import * as trans from '../transformations'

test("filterTracks should return tracks that are longer than 1 minute and have a name", () =>
{
    const input = [
        { id: 1, name: 'short song', duration_ms: 42 },
        // nameless song
        { id: 2, name: null, duration_ms: 80000 },
        { id: 3, name: 'valid song', duration_ms: 90000 },
      ];
  
      const expectedOutput = [
        { id: 3, name: 'valid song', duration_ms: 90000 },
      ];

    expect(trans.filterTracks(input)).toEqual(expectedOutput);
})