# Rekordbox Fields

* [Rekordbox database format](https://pyrekordbox.readthedocs.io/en/latest/formats/xml.html)
 
## Primary key: `Location`

* `Location`: Note that, thanks to the filesystem's case insensitivity, some of these do not match what's actually used on disk (same with beets data) so we force it to lower case.

## beets source of truth

These should normally be controlled by beets and written to rekordbox

 * `Name`
 * `Artist`
 * `Composer`
 * `Album`
 * `Grouping`
 * `Genre`
 * `DiscNumber`
 * `TrackNumber`
 * `Year`
 * `Comments`
 * `Label`

## rekordbox additional metadata

New fields to sync by default into beets from rekordbox.

Implemented: 

 * `TrackID`: Rekordbox internal track identifier number, note that I think this is not viewable anymore in the GUI and is back-end only
 * `Rating`: default 0
 * `DateAdded`: Date track added to rekordbox, useful in many contexts to preserve
 * `PlayCount`
 * `Remixer`
 * `Mix`: Track metadata that doesn't have a key in beets
 
TODO:

 * `Colour`: User-set color identifiers

# Rekordbox analyzed metadata

These should (?) be computed when rekordbox does an analysis job and therefore rekordbox should be the source of truth.

 * `Tonality`: Key encoding, computed from analysis
 * `AverageBpm`

## Properties

 * `LastPlayed`: seems to be all `<NA>` in my data, ignoring for now...

These should be computed from the data. 

 * `Kind`: `['MP3 File', 'M4A File', 'WAV File']`
 * `Size`
 * `TotalTime`
 * `DateModified`: Unclear if this is filesystem or rekordbox or what, "Date of last modification"
 * `BitRate`
 * `SampleRate`