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


describe("explodeDateField", () => {
    it("should update the original json with 3 additional fields: year, month, day, assigned with values based on provided date field", () => {
        const json_input = { id: 1, release_date: '2013-06-13', record_date: '2013-06-12' };

        const dateField_input = 'release_date';

        const json_expected_output = { id: 1, release_date: '2013-06-13', record_date: '2013-06-12', year: 2013, month: 6, day: 13 };

        trans.explodeDateField(json_input, dateField_input);

        expect(json_input).toEqual(json_expected_output);
    });

    it("should update the original json with 3 additional fields: year, month, day, assigned with null values for missing day", () => {
        const json_input = { id: 1, release_date: '2013-06' };

        const dateField_input = 'release_date';

        const json_expected_output = { id: 1, release_date: '2013-06', year: 2013, month: 6, day: null };

        trans.explodeDateField(json_input, dateField_input);

        expect(json_input).toEqual(json_expected_output);
    });

    it("should update the original json with 3 additional fields: year, month, day, assigned with null values for missing month and day", () => {
        const json_input = { id: 2, release_date: '2025' };

        const dateField_input = 'release_date';

        const json_expected_output = { id: 2, release_date: '2025', year: 2025, month: null, day: null };

        trans.explodeDateField(json_input, dateField_input);

        expect(json_input).toEqual(json_expected_output);
    });

    it("should not add the year/month/day fields to the json if dateField is null", () => {
        const json_input = { id: 1, release_date: null };

        const dateField_input = 'release_date';

        const json_expected_output = { id: 1, release_date: null };

        trans.explodeDateField(json_input, dateField_input);

        expect(json_input).toEqual(json_expected_output);
    });
});
