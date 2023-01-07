# Rekordbox Fields

* [Rekordbox database format](https://pyrekordbox.readthedocs.io/en/latest/formats/xml.html)
 
## Primary key: `Location`

* `Location`: Forced to lowercase because the filesystem I'm testing this on is in lowercase.
  * Rekordbox also strips the root `/` which we add back in.
  * Making this correctly portable will be some amount of work.

## beets source of truth

These should be controlled by beets and written to rekordbox

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

 * `AverageBpm`
 * `Colour`: User-set color identifiers
 * `DateAdded`: Date track added to rekordbox, useful to preserve
 * `DateModified`
 * `LastPlayed`
 * `Mix`: Track metadata that doesn't have a key in beets
 * `PlayCount`: How many times it has been played in Rekordbox
 * `Rating`: Default 0 for no rating
 * `Tonality`: Key encoding, computed from analysis
 * `TrackID`: Rekordbox internal track identifier number, note that I think this is not viewable anymore in the GUI and is back-end only

# Rekordbox analyzed metadata

 * `Kind`: `['MP3 File', 'M4A File', 'WAV File']` unclear if there are more
 * `Size`
 * `TotalTime`
 * `BitRate`
 * `SampleRate`