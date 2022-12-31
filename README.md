# rkbeets

A [beets](https://beets.io/) plugin for simple integration with rekordbox.

Python 3.10. Be careful - **use a copy of your database** - there are no confirmation flows.

## Quick tour

_Tested only on recent macos._

### Data input and output

* The plugin will export data for import into rekordbox using the option `export-file`.
* Some commands use data from rekordbox, provided to beets via the option `rekordbox-file`.
* Setting the configuration options cleans up the command line, e.g.:

`.config/beets/config.yaml`:
```yaml 
rkbeets:
    export-file: ~/Documents/rekordbox/rekordbox.xml
    rekordbox-file: ~/Documents/rekordbox/export.xml
```

### Export beets to rekordbox with `rkb-export`

Export your beets library for import into rekordbox with `rkb-export`:

```sh
# beets library -exported-to-> rekordbox
beet rkb-export

# only export files missing from rekordbox, requires `rekordbox-file`
beet rkb-export --missing

# export only missing files further filtered by a query
beet rkb-export artist:radiohead --missing
```

### Inspect beets and rekordbox differences with `rkb-diff`


Inspect how many tracks are shareed between the two libraries (and a list of those that aren't) with `rkb-diff`:

```sh
# rekordbox exported xml -compared-to-> beets library
beet rkb-diff
```

Tracks are matched between the two only by using file paths.

### Copy metadata from rekordbox into beets with `rkb-sync`

The `rkb-sync` command lets you pull metadata from rekordbox into beets. 

```sh
# rekordbox metadata -written-to-> beets library
beet rkb-sync

# only consider shared tracks that satisfy a query
beet rkb-sync artist:radiohead
```

Currently implemented for these fields:

* `Rating`
* `TrackID`
* `DateAdded`
* `PlayCount`
* `Remixer`
* `Mix`

### Importing from rekordbox to beets

Import files using `beets import`.

# Installation

Since it is under development, use source:

* Clone [beets](https://github.com/beetbox/beets) and [rkbeets](https://github.com/voigtjr/rkbeets) next to each other
* Create a virtual environment or similar in `beets` repo: `python -m venv .venv`
* Install `beets` into that environment
* `pip install -e ../rkbeets`
* `beet rkb-diff` and friends should work

# License

See [LICENSE](LICENSE).
