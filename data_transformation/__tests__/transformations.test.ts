import * as trans from '../transformations'

describe("filterTracks", () => {
    it("should return tracks that are longer than 1 minute and have a name", () => {
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

describe("filterArtists", () => {
    it("should return only artists that have tracks in the filtered tracks list", () => {
        const artists_input = [
            { id: 1, name: 'Artist A' },
            { id: 2, name: 'Artist "Nickname" B' },
            { id: 3, name: 'Artist A\'s son' },
        ];
        const artistsFromTracks_input =
            new Set(['Artist A', 'Artist A\'s son']);

        const expectedOutput = [
            { id: 1, name: 'Artist A' },
            { id: 3, name: 'Artist A\'s son' },
        ];

        expect(trans.filterArtists(artists_input, artistsFromTracks_input)).toEqual(expectedOutput);
    });

    it('should return an empty array if no artists were in the artistsFromTracks list', () => {
        const artists_input = [
            { id: 1, name: 'Artist A' },
            { id: 2, name: 'Artist "Nickname" B' },
            { id: 3, name: 'Artist A\'s son' },
        ];
        const artistsFromTracks_input =
            new Set([]);

        expect(trans.filterArtists(artists_input, artistsFromTracks_input)).toEqual([]);
    });
});

describe("parseDateParts", () => {
    it("should return 3 fields: year, month, day, assigned with values from the input", () => {
        const input = ["2013", "06", "13"];

        const year = 2013;
        const month = 6;
        const day = 13;

        expect(trans.parseDateParts(input)).toEqual({ year, month, day });
    });

    it("should return 3 fields: year, month, day, with null assigned to missing day", () => {
        const input = ["2013", "06"];

        const year = 2013;
        const month = 6;
        const day = null;

        expect(trans.parseDateParts(input)).toEqual({ year, month, day });
    });

    it("should return 3 fields: year, month, day, with null assigned to missing month and day", () => {
        const input = ["2013"];

        const year = 2013;
        const month = null;
        const day = null;

        expect(trans.parseDateParts(input)).toEqual({ year, month, day });
    });
});
