import * as trans from '../transformations'

describe("filterTracks", () => {
    it("should return tracks that are longer than 1 minute and have a name", () =>
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
        });

    it('should return an empty array if all tracks are invalid', () => {
        const input = [
          { name: null, duration_ms: 60000 },
          { name: 'Song A', duration_ms: 59999 },
        ];
        
        expect(trans.filterTracks(input)).toEqual([]);
      });
});
