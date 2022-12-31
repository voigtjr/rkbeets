# rkbeets

A plugin for integrating Rekordbox XML and [Beets](https://beets.io/).

## Quick tour

_Tested only on recent macos._

If you are just starting with Rekordbox, you can export your beets library for import into Rekordbox. Set `rkbeets.xml_outfile` and:

```sh
beet rkb-make-import

# Or, using query results
beet rkb-make-import artist:radiohead
```

If you export your Rekordbox library and set the configuration key `rkbeets.xml_outfile`, you can see how many tracks you have in each (considering only the configured beets music directory):

```sh
beet rkb-report

# To see the paths for tracks not in both libraries
beet rkb-report --verbose
```

Tracks are matched between Rekordbox and beets using file paths.

Although only currently implemented for the 'Rating' field, this command lets you sync metadata from Rekordbox to beets:

```sh
beet rkb-sync
beet rkb-sync artist:radiohead
```

Be careful - **use a copy of your database** - confirmation flows are not implemented.

# Installation

Since it is under heavy development:

* Clone [beets](https://github.com/beetbox/beets) and [rkbeets](https://github.com/voigtjr/rkbeets) next to each other
* Create a virtual environment or similar in `beets` repo: `python -m venv .venv`
* Install `beets` into that environment
* `pip install -e ../rkbeets`
* `beet rkb-report` and friends should work

# License

See [LICENSE](LICENSE).
