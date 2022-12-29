# rkbeets

A plugin for integrating Rekordbox XML and [Beets](https://beets.io/).

# Implemented overview

Tracks are matched from Rekordbox to beets using file paths.

```sh
# Specify at command line or in configuration file
beet rkbeets -x /path/to/exported.xml

# Show information about beets/rbxml
beet rkbeets report

# Copy metadata into beets from tracks found in rekordbox
beet rkbeets sync
```

# Planned interfaces

```sh
# Generate XML file to import into rekordbox
beet rkbeets make-import --missing # Tracks in beets but missing in rb
beet rkbeets make-import --missing artist:dream # Same but with a query
beet rkbeets make-import # Everything
```

## Interesting and related work

- [Structured comments plugin](https://github.com/michaeltoohig/BeetsPluginStructuredComments)
- [Describe plugin](https://github.com/adamjakab/BeetsPluginDescribe)
- [Xtractor plugin](https://github.com/adamjakab/BeetsPluginXtractor)
- [Pyrekordbox](https://pyrekordbox.readthedocs.io/en/latest/)

# License

See [LICENSE](LICENSE).
