| rkb field | rkb type | beets field | beets type | comment |
| --------- | -------- | ----------- | ---------- | ------- |
| `Album` | `string` | `album` | `string` |
| `Artist` | `string` | `artist` | `string` |
| `AverageBpm` | `float64` | | | not yet captured |
| `BitRate` | `int32` | `bitrate` | `int32` |
| `Colour` | `string` | | | not yet captured |
| `Comments` | `string` | `comments` | `string` |
| `Composer` | `string` | `composer` | `string` |
| `DateAdded` | `string` | `rkb-DateAdded` | `string` | formatted |
| `DateModified` | `string` | | | not yet captured, formatted |
| `DiscNumber` | `int32` | `disc` | `int32` |
| `Genre` | `string` | `genre` | `string` |
| `Grouping` | `string` | `grouping` | `string` |
| `Kind` | `string` | `format` | `string` | requires conversion |
| `Label` | `string` | `label` | `string` |
| `LastPlayed` | `string` | | | not yet captured |
| `Location` | `string` | `path` | `bytes` | beets requires decode |
| `Mix` | `string` | `rkb-Mix` | `string` |
| `Name` | `string` | `title` | `string` |
| `PlayCount` | `int32` | `rkb-PlayCount` | `Int32` | beets nullable |
| `Rating` | `int32` | `rkb-Rating` | `Int32` | beets nullable |
| `Remixer` | `string` | `remixer` | `string` |
| `SampleRate` | `float64` | `samplerate` | `int32` | requires conversion |
| `Size` | `int64` | `filesize` | `int64` |
| `Tonality` | `string` | | | not yet captured |
| `TotalTime` | `float64` | `length` | `int32` |
| `TrackID` | `int64` | `rkb-TrackID` | `Int64` | rkb internal, beets nullable |
| `TrackNumber` | `int32` | `track` | `int32` |
| `Year` | `int32` | `year` | `int32` |
| | | `id` | `int32` | beets-internal |

## References

- [Pioneer xml format list](https://cdn.rekordbox.com/files/20200410160904/xml_format_list.pdf)
- [Pyrekordbox documentation](https://pyrekordbox.readthedocs.io/en/latest/formats/xml.html)
