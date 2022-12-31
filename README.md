# rkbeets

A [beets](https://beets.io/) plugin for simple integration with rekordbox.

Python 3.10. Be careful - **use a copy of your database** - there are no confirmation flows.

## Quick tour

_Tested only on recent macos._

### Export beets to rekordbox with `rkb-export`

You can export your beets library for import into rekordbox with `rkb-export`:

```sh
# beets library -exported-to-> rekordbox xml
beet rkb-export --export-file ~/Documents/rekordbox/rekordbox.xml

# export using query results and the export-file set in beets config
beet rkb-export artist:radiohead
```

### Inspect beets and rekordbox differences with `rkb-diff`

If you export your rekordbox library, you can see how many tracks they share in the beets media directory tree and lists of those tracks that aren't shared, with `rkb-diff`:

```sh
# rekordbox exported xml -compared-to-> beets library
beet rkb-diff --import-file ~/Documents/export.xml
```

Tracks are matched between rekordbox and beets only by using file paths.

### Copy metadata from rekordbox into beets with `rkb-sync`

(Currently only implemented for the rekordbox `Rating` field.)

The `rkb-sync` command lets you pull metadata from rekordbox into beets. 

```sh
# rekordbox metadata -written-to-> beets library
beet rkb-sync

# only consider shared tracks that satisfy a query
beet rkb-sync artist:radiohead
```

### Importing from rekordbox to beets

Just import the files normally using the `import` command.

# Installation

Since it is under development, use source:

* Clone [beets](https://github.com/beetbox/beets) and [rkbeets](https://github.com/voigtjr/rkbeets) next to each other
* Create a virtual environment or similar in `beets` repo: `python -m venv .venv`
* Install `beets` into that environment
* `pip install -e ../rkbeets`
* `beet rkb-diff` and friends should work

# License

See [LICENSE](LICENSE).
