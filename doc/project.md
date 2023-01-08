* A flag to dump export xml to stdout for command line composition
* A report on differences in filesystem path case for paths in libraries vs actual paths on disk
* Preserve NAs in sync command instead of using default values, but this has a bit more intense matching logic.
* Figure out the minimal thing to do to sync beets metadata instead of calling `try_sync` unless that is, in fact, idiomatic
