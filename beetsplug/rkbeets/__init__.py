# Copyright (c) 2022, Jonathan Voigt
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from collections import namedtuple
from functools import cached_property, reduce
from importlib import resources
import logging
import operator
from pathlib import Path
from typing import Any, Callable, Iterable

from beets import config
from beets import plugins
from beets.dbcore import db, types
from beets import ui
from beets import library
import pandas
from tqdm import tqdm

import beetsplug
import beetsplug.rkbeets

# pyrekordbox is chatty about missing/broken rekordbox configuration files
previous_level = logging.root.manager.disable
logging.disable(logging.CRITICAL)
try:
    import pyrekordbox.xml as pxml # type: ignore
finally:
    logging.disable(previous_level)

class DimensionsDB():
    """
    Manages metadata about beets and rekordbox fields and their relationships to
    one another.
        
    Parameters
    ----------
    csv: Path, optional
        Use this csv file instead of the default.
    """

    _df: pandas.DataFrame

    def __init__(self, csv_path: Path | None = None):
        path = csv_path if csv_path is not None else resources.path(
            beetsplug.rkbeets, 'rkbeets-fields.csv')
        self._df = pandas.read_csv(path)
        if self._df.index.has_duplicates:
            # TODO test this works
            raise RuntimeError("rkbeets-fields contains duplicate columns between library types")
    
    def to_pickle(self, dir: Path) -> None:
        """
        Pickle the DataFrame to `ddb.pkl` in the given directory.
        
        Parameters
        ----------
        dir: Path
            Directory to write 'ddb.pkl'
        """

        self._df.to_pickle(dir / Path('ddb.pkl'))
    
    def num_beets_cols(self) -> int:
        """The number of beets fields."""

        return self._df['beets_field'].dropna().size
    
    def get_beets_cols(self) -> Iterable[tuple[str, str]]:
        """Returns a namedtuple of beets `field`s and their corresponding `dtype`s."""

        df = self._df[['beets_field', 'beets_type']].dropna()
        mapping = {
            'beets_field': 'field',
            'beets_type': 'dtype',
        }
        return df.rename(columns=mapping).itertuples(name='FieldInfo', index=False)

    def num_rkb_cols(self) -> int:
        """The number of rekordbox fields."""

        return self._df['rkb_field'].dropna().size
    
    def get_rkb_cols(self) -> Iterable[tuple[str, str]]:
        """Returns a namedtuple of rekordbox `field`s and their corresponding `dtype`s."""

        df = self._df[['rkb_field', 'rkb_type']].dropna()
        mapping = {
            'rkb_field': 'field',
            'rkb_type': 'dtype',
        }
        return df.rename(columns=mapping).itertuples(name='FieldInfo', index=False)

    def get_export_conversion_info(
        self
    ) -> tuple[list[str], tuple[str, str, Callable[[Any], Any] | None]]:
        """
        Get the information required to transform beets metadata to rekordbox
        metadata to export from beets to import into rekordbox.

        Returns
        -------
        drop_fields : list
            List of beets fields to drop as they are not exported to rekordbox
        export_fields: iterator
            A named tuple `ExportFields` with the `beets` field to rename to
            `rkb` field name, and an optional function `func` to use to
            transform the values.
        """
    
        no_export = self._df['no_export'].fillna(False)
        drop_fields = self._df[no_export]['beets_field'].dropna().tolist()

        def format_to_kind(format: str) -> str:
            mapping = {
                'AAC': 'M4A File',
                'MP3': 'MP3 File',
                'WAV': 'WAV File',
                # Unclear if there are more types...
            }
            kind = mapping.get(format)
            return kind if kind is not None else format

        xform = {
            'format_to_kind': format_to_kind
        }

        df_export = self._df[~no_export][
            ['beets_field', 'rkb_field', 'convert_to_rkb']
            ].dropna(subset=['beets_field', 'rkb_field'])

        ef = namedtuple("ExportFields", ['beets', 'rkb', 'func'])
        export_fields = (
            ef._make([row.beets_field, row.rkb_field, xform.get(row.convert_to_rkb)])
            for row in df_export.itertuples(index=False))

        ei = namedtuple("ExportInfo", ['drop_fields', 'export_fields'])
        return ei._make([drop_fields, export_fields])    

    def get_sync_pairs(self) -> Iterable[tuple[str, str]]:
        """
        Get corresponding beets and rekordbox fields.

        Returns
        -------
        FieldPairs : iterator
            namedtuples with `beets` and corresponding `rkb` fields.

        """
        df = self._df[self._df['sync'].fillna(False)]
        df = df[['beets_field', 'rkb_field']].rename(columns={
            'beets_field': 'beets',
            'rkb_field': 'rkb',
        })
        return df.itertuples(name='FieldPairs', index=False)


ComputedLibraries: tuple[
    pandas.DataFrame, pandas.Index, pandas.Index
] = namedtuple('ComputedLibraries', ['df_common', 'only_beets', 'only_rbxml'])

class Libraries():
    """
    Manages beets and rekordbox libraries and the operations between them
    delegating a ton of actual work to dataframes.

    Loads the libraries on demand, so, if something isn't configured correctly,
    it won't error out until it is used.
    
    Parameters
    ----------
    lib: Library
        Beets library.
    query: beets command line query, optional
        The query from the command line arguments, usually from `ui.decargs`.
    xml_path: Path
        Path to the exported rekordbox xml library that will be read for various operations.
    ddb_csv_path: Path
        Override data for the dimensions db, for testing.
    """

    _ddb: DimensionsDB
    _items: db.Results
    _xml_path = Path
    
    def __init__(
        self, lib: library.Library,
        query: str | list | tuple,
        xml_path: Path = None,
        ddb_csv_path: Path = None,
    ):
        self._ddb = DimensionsDB(csv_path=ddb_csv_path)
        self._items = lib.items(query)
        self._xml_path = xml_path

    @cached_property
    def _df_beets(self) -> pandas.DataFrame:
        print("Loading beets library metadata...")
        with tqdm(total=self._ddb.num_beets_cols(), unit='columns') as pbar:
            def get_series(cols):
                series = pandas.Series(
                    data=[i.get(cols.field) for i in self._items],
                    dtype=cols.dtype
                )
                pbar.update()
                return series

            series_data = {
                cols.field: get_series(cols)
                for cols in self._ddb.get_beets_cols()
            }

        df = pandas.DataFrame(data=series_data)
        index = df['path'].str.decode('utf-8').str.normalize('NFD').str.lower()
        return df.set_index(index)

    def beets_track_count(self) -> int:
        """
        Returns the number of beets track loaded, subject to the query if any.
        """

        return self._df_beets.index.size
    
    @cached_property
    def _df_rbxml(self) -> pandas.DataFrame:
        xml = pxml.RekordboxXml(self._xml_path)
        tracks = xml.get_tracks()

        print("Loading rekordbox xml...")
        with tqdm(total=self._ddb.num_rkb_cols(), unit='columns') as pbar:
            def get_series(cols):
                series = pandas.Series(
                    data=[t[cols.field] for t in tracks],
                    dtype=cols.dtype
                )
                pbar.update()
                return series

            series_data = {
                cols.field: get_series(cols)
                for cols in self._ddb.get_rkb_cols()
            }

        df = pandas.DataFrame(data=series_data)

        # Prepend a slash to the paths, Rekordbox removes this
        df['Location'] = '/' + df['Location']

        index = df['Location'].str.normalize('NFD').str.lower()
        return df.set_index(index)

    def to_pickle(self, dir: Path) -> None:
        """
        Pickle the beets and rekordbox `DataFrame`s to `df_beets.pkl` and
        `df_rbxml.pkl` in the given directory. Call `to_pickle` on ddb.
        
        Parameters
        ----------
        dir: Path
            Directory to write pickle files.
        """

        self._df_beets.to_pickle(dir / Path('df_beets.pkl'))
        self._ddb.to_pickle(dir)
        if self._df_rbxml is not None:
            self._df_rbxml.to_pickle(dir / Path('df_rbxml.pkl'))

    def crop(self, music_directory: str | None = None) -> ComputedLibraries:
        """
        Compare the two libraries using only filesystem paths. If a
        `music_directory` is given, only consider files to be missing from the
        beets library if they are present in that tree. Join all the
        fields and return that, as well as lists of which files are only in
        each.

        Parameters
        ----------
        music_directory: str, optional
            The configured music directory for beets files, usually straight
            from config.
        
        Returns
        -------
        df_common: pandas.DataFrame
            All files common to both libraries with all fields in both
            repositories.
        only_beets: pandas.Index
            Paths only in beets library.
        only_rbxml: pandas.Index
            Paths only in rekordbox library.
        """

        df_r = self._df_rbxml
        if music_directory:
            # Filter tracks outside of music directory
            i = self._df_rbxml.index.str.startswith(music_directory.lower())
            df_r = self._df_rbxml[i]

        only_rbxml = df_r.index.difference(self._df_beets.index)
        only_beets = self._df_beets.index.difference(df_r.index)

        intersection = df_r.index.intersection(self._df_beets.index)
        df_common = self._df_beets.loc[intersection].join(
            df_r.loc[intersection]
        )

        return ComputedLibraries(df_common=df_common, only_beets=only_beets, only_rbxml=only_rbxml)

    def get_export_df(self, index: pandas.Index | None = None) -> pandas.DataFrame:
        """
        Get the dataframe of tracks to export to rekordbox. Renames and converts
        field values using the dimensions db.

        Parameters
        ----------
        index: pandas.Index, optional
            If present, filter using this index of file paths instead of
            considering the entire library.
        
        Returns
        -------
        df: pandas.DataFrame
            The tracks indexed by paths including all field columns with
            converted values ready for pyrekordbox xml api.
        """
        df_beets = self._df_beets if index is None else self._df_beets.loc[index]

        export_info = self._ddb.get_export_conversion_info()
        df = df_beets.drop(columns=export_info.drop_fields)
        df = df.rename(columns={
            row.beets: row.rkb
            for row in export_info.export_fields
        }, errors='raise')

        # Use the type's default value to fill the nulls
        for field, value in df.dtypes.items():
            if value.type() is None:
                continue
            df[field] = df[field].fillna(value=value.type())

        # Required conversions
        for row in export_info.export_fields:
            if row.func is not None:
                df[row.rkb] = df[row.rkb].transform(row.func)

        return df

    def get_sync_changed(self, df_common: pandas.DataFrame) -> pandas.DataFrame:
        """
        Compare the columns that are marked to sync and include them in the
        returned data if they are changed.

        Parameters
        ----------
        df_common: pandas.DataFrame
            Output from crop, potentially filtered.
        
        Returns
        -------
        df_changed: pandas.DataFrame
            The changed items to sync, indexed by beets `id` field, with NAs
            filled with default values.
        """
        def ne(l, r):
            return l.fillna(l.dtype.type()) != r

        compares = (
            ne(df_common[cols.beets], df_common[cols.rkb])
            for cols in self._ddb.get_sync_pairs()
        )
        mask = reduce(operator.or_, compares)
        df_changed = df_common[mask]
        df_changed = df_changed.set_index('id')

        def transform_column(cols):
            default = df_common[cols.beets].dtype.type()
            return df_changed[cols.rkb].fillna(default)

        return pandas.DataFrame(data={
            cols.beets: transform_column(cols) 
            for cols in self._ddb.get_sync_pairs()
        }) 


def export_df(xml_path: Path, df: pandas.DataFrame) -> None:
    """
    Convert a dataframe filled with rekordbox metadata into xml.

    Parameters
    ----------
    xml_path: Path
        Output xml file path.
    df: pandas.DataFrame
        Input data.    
    """
    outxml = pxml.RekordboxXml(
        name='rekordbox', version='5.4.3', company='Pioneer DJ'
    )

    # Strip leading slash because rekordbox doesn't like it
    locations = df['Location'].str.slice(1)

    # Simplify loop by extracting this now
    df = df.drop(columns=['Location'])

    print("Rendering {} tracks...".format(df.index.size))
    with tqdm(total=df.index.size, unit='tracks') as pbar:
        for row, location in zip(df.itertuples(index=False), locations):
            outxml.add_track(
                location=location,
                # Filter empty strings
                **{ k: v for k, v in row._asdict().items() if v != '' }
            )
            pbar.update()

    print("Writing {}...".format(xml_path))
    outxml.save(path=xml_path)


class RkBeetsPlugin(plugins.BeetsPlugin):
    """
    Integrate beets and rekordbox using rekordbox exported xml library data.

    Configuration
    -------------
    rkbeets.export_file: Path
        The plugin will export data for import into rekordbox using an xml file
        written to this path.
    rkbeets.rekordbox_file: Path
        Some commands use data exported from rekordbox into an xml file at this
        path.
    """

    # beets plugin interface to declare flexible attr types
    item_types: dict[str, types.Type] = {
        'rkb_AverageBpm': types.FLOAT,
        'rkb_Colour': types.STRING,
        'rkb_DateAdded': types.STRING,
        'rkb_DateModified': types.STRING,
        'rkb_LastPlayed': types.STRING,
        'rkb_Mix': types.STRING,
        'rkb_PlayCount': types.INTEGER,
        'rkb_Rating': types.INTEGER,
        'rkb_Tonality': types.STRING,
        'rkb_TrackID': types.INTEGER,
    }

    def __init__(self):
        super().__init__()

        self.config.add({
            'export_file': None,
            'rekordbox_file': None,
        })

    def commands(self) -> list[Callable[[library.Library, Any, Any], Any]]:
        """
        Returns a small set of commands, all prefixed with `rkb-`, for addition
        to the beets cli.
        """

        def rkb_export_func(lib: library.Library, opts, args):
            """export beets library for import into rekordbox"""

            self.config.set_args(opts)
            export_path = self.config['export_file'].get()

            libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            index = None
            if opts.missing:
                cl = libs.crop(config['directory'].get())

                if cl.only_beets.empty:
                    print("nothing to do: no tracks are missing from rekordbox")
                    return

                index = cl.only_beets

            df_export = libs.get_export_df(index)

            export_df(export_path, df_export)

        rkb_export_cmd = ui.Subcommand(
            'rkb-export',
            help=rkb_export_func.__doc__
        )
        rkb_export_cmd.func = rkb_export_func
        rkb_export_cmd.parser.add_option(
            '-e', '--export-file', dest='export_file',
            help="target file for beets data exported for rekordbox"
        )
        rkb_export_cmd.parser.add_option(
            '-r', '--rekordbox-file', dest='rekordbox_file',
            help="rekordbox xml library"
        )
        rkb_export_cmd.parser.add_option(
            '-m', '--missing', dest='missing', action='store_true', default=False,
            help="only consider files not already in rekordbox library"
        )

        def rkb_diff_func(lib: library.Library, opts, args):
            """show information and differences between the rekordbox and beets libraries"""

            self.config.set_args(opts)

            libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            if opts.pickle:
                print("Writing dataframes to {}".format(opts.pickle))
                libs.to_pickle(opts.pickle)

            cl = libs.crop(config['directory'].get())

            print("{:>6d} tracks in rekordbox library (in beets directory)".format(
                cl.df_common.index.size + cl.only_rbxml.size))
            print("{:>6d} tracks in beets library (subject to query if any)".format(libs.beets_track_count()))
            print("{:>6d} shared tracks in both".format(cl.df_common.index.size))

            if not cl.only_rbxml.empty:
                print("Only in Rekordbox:")
                for path in cl.only_rbxml:
                    print("    ", path)

            if not cl.only_beets.empty:
                print("Only in beets:")
                for path in cl.only_beets:
                    print("    ", path)

        rkb_diff_cmd = ui.Subcommand(
            'rkb-diff',
            help=rkb_diff_func.__doc__
        )
        rkb_diff_cmd.func = rkb_diff_func
        rkb_diff_cmd.parser.add_option(
            '-r', '--rekordbox-file', dest='rekordbox_file',
            help="rekordbox xml library"
        )
        rkb_diff_cmd.parser.add_option(
            '--pickle',
            help="export dataframes to given directory"
        )

        def rkb_sync_func(lib: library.Library, opts, args):
            """sync metadata from rekordbox xml to beets database"""

            self.config.set_args(opts)

            libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            cl = libs.crop(config['directory'].get())

            df_sync_changed = libs.get_sync_changed(cl.df_common)

            if df_sync_changed.empty:
                print("nothing to update")
                return

            print("Updating {} tracks...".format(df_sync_changed.index.size))
            with tqdm(total=df_sync_changed.index.size, unit='tracks') as pbar:
                for row in df_sync_changed.itertuples():
                    data = row._asdict()
                    id = data.pop('Index')
                    item = lib.get_item(id)
                    item.update(data)

                    if opts.dry_run:
                        print("{} --> {}".format(item.get('path').decode('utf-8'), data))
                    else:
                        item.try_sync(False, False)
                        pbar.update()

        rkb_sync_cmd = ui.Subcommand(
            'rkb-sync',
            help=rkb_sync_func.__doc__
        )
        rkb_sync_cmd.func = rkb_sync_func
        rkb_sync_cmd.parser.add_option(
            '-r', '--rekordbox-file', dest='rekordbox_file',
            help="rekordbox xml library"
        )
        rkb_sync_cmd.parser.add_option(
            '-n', '--dry-run', dest='dry_run', action='store_true', default=False,
            help="print the changes instead of committing them"
        )

        return [rkb_export_cmd, rkb_diff_cmd, rkb_sync_cmd]
