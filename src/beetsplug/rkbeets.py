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

from collections import namedtuple, abc
from functools import cached_property, reduce
from importlib import resources
import logging
import operator
from pathlib import Path
from typing import Any, Callable, Final, Iterable, TextIO, Tuple

from beets import config
from beets import plugins
from beets.dbcore import db, types
from beets import ui
from beets import library
import pandas
from tqdm import tqdm

import beetsplug

VERSION = '0.1.0'

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
    csv_buffer: TextIO, optional
        Override the default and read dimensions CSV from this buffer
        instead
    """

    _df: pandas.DataFrame

    def __init__(self, csv_buffer: TextIO | None = None):
        if csv_buffer:
            self._df = pandas.read_csv(csv_buffer)
        else:
            with resources.open_text(beetsplug, 'rkbeets-fields.csv') as f:
                self._df = pandas.read_csv(f)
    
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
        drop_fields = self._df[no_export]['beets_field'].tolist()

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
        df = self._df[self._df['beets_field'].str.startswith('rkb_').fillna(False)]
        df = df[['beets_field', 'rkb_field']].rename(columns={
            'beets_field': 'beets',
            'rkb_field': 'rkb',
        })
        return df.itertuples(name='FieldPairs', index=False)


ComputedLibraries = namedtuple('ComputedLibraries', ['df_common', 'only_beets', 'only_rbxml'])

class Libraries():
    ddb: DimensionsDB
    items: db.Results
    xml_path = Path
    
    def __init__(
        self, lib: library.Library, query: str | list | tuple,
        xml_path: Path = None
    ):
        self.ddb = DimensionsDB()
        self.items = lib.items(query)
        self.xml_path = xml_path

    @cached_property
    def df_beets(self) -> pandas.DataFrame:
        print("Loading beets library metadata...")
        with tqdm(total=self.ddb.num_beets_cols(), unit='columns') as pbar:
            def get_series(cols):
                series = pandas.Series(
                    data=[i.get(cols.field) for i in self.items],
                    dtype=cols.dtype
                )
                pbar.update()
                return series

            series_data = {
                cols.field: get_series(cols)
                for cols in self.ddb.get_beets_cols()
            }

        df = pandas.DataFrame(data=series_data)
        index = df['path'].str.decode('utf-8').str.normalize('NFD').str.lower()
        return df.set_index(index)
    
    @cached_property
    def df_rbxml(self) -> pandas.DataFrame:
        xml = pxml.RekordboxXml(self.xml_path)
        tracks = xml.get_tracks()

        print("Loading rekordbox xml...")
        with tqdm(total=self.ddb.num_rkb_cols(), unit='columns') as pbar:
            def get_series(cols):
                series = pandas.Series(
                    data=[t[cols.field] for t in tracks],
                    dtype=cols.dtype
                )
                pbar.update()
                return series

            series_data = {
                cols.field: get_series(cols)
                for cols in self.ddb.get_rkb_cols()
            }

        df = pandas.DataFrame(data=series_data)

        # Prepend a slash to the paths, Rekordbox removes this
        df['Location'] = '/' + df['Location']

        index = df['Location'].str.normalize('NFD').str.lower()
        return df.set_index(index)

    def to_pickle(self, dir: Path) -> None:
        self.df_beets.to_pickle(dir / Path('df_beets.pkl'))
        self.ddb.to_pickle(dir)
        if self.df_rbxml is not None:
            self.df_rbxml.to_pickle(dir / Path('df_rbxml.pkl'))

    def crop(self, music_directory: str | None = None) -> ComputedLibraries:
        df_r = self.df_rbxml
        if music_directory:
            # Filter tracks outside of music directory
            i = self.df_rbxml.index.str.startswith(music_directory.lower())
            df_r = self.df_rbxml[i]

        only_rbxml = df_r.index.difference(self.df_beets.index)
        only_beets = self.df_beets.index.difference(df_r.index)

        intersection = df_r.index.intersection(self.df_beets.index)
        df_common = self.df_beets.loc[intersection].join(
            df_r.loc[intersection]
        )

        return ComputedLibraries(df_common=df_common, only_beets=only_beets, only_rbxml=only_rbxml)

    def get_export_df(self, index: pandas.Index | None = None) -> pandas.DataFrame:
        df_beets = self.df_beets if index is None else self.df_beets[index]

        export_info = self.ddb.get_export_conversion_info()
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
        def ne(l, r):
            return l.fillna(l.dtype.type()) != r

        compares = (
            ne(df_common[cols.beets], df_common[cols.rkb])
            for cols in self.ddb.get_sync_pairs()
        )
        mask = reduce(operator.or_, compares)
        df_changed = df_common[mask]
        df_changed = df_changed.set_index('id')

        def transform_column(cols):
            default = df_common[cols.beets].dtype.type()
            return df_changed[cols.rkb].fillna(default)

        return pandas.DataFrame(data={
            cols.beets: transform_column(cols) 
            for cols in self.ddb.get_sync_pairs()
        }) 


def export_df(xml_path, df: pandas.DataFrame) -> None:
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
        rkb_export_cmd = ui.Subcommand(
            'rkb-export',
            help="export beets library for import into rekordbox"
        )

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

        def rkb_export_func(lib: library.Library, opts, args):
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

        rkb_export_cmd.func = rkb_export_func

        rkb_diff_cmd = ui.Subcommand(
            'rkb-diff',
            help='show information and differences between the rekordbox and beets libraries'
        )

        rkb_diff_cmd.parser.add_option(
            '-r', '--rekordbox-file', dest='rekordbox_file',
            help="rekordbox xml library"
        )

        rkb_diff_cmd.parser.add_option(
            '--pickle',
            help="export dataframes to given directory"
        )

        def rkb_diff_func(lib: library.Library, opts, args):
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
            print("{:>6d} tracks in beets library".format(libs.df_beets.index.size))
            print("{:>6d} shared tracks in both".format(cl.df_common.index.size))

            if not cl.only_rbxml.empty:
                print("Only in Rekordbox:")
                for path in cl.only_rbxml:
                    print("    ", path)

            if not cl.only_beets.empty:
                print("Only in beets:")
                for path in cl.only_beets:
                    print("    ", path)

        rkb_diff_cmd.func = rkb_diff_func

        rkb_sync_cmd = ui.Subcommand(
            'rkb-sync', help='sync metadata from rekordbox xml to beets database'
        )
        rkb_sync_cmd.parser.add_option(
            '-r', '--rekordbox-file', dest='rekordbox_file',
            help="rekordbox xml library"
        )
        rkb_sync_cmd.parser.add_option(
            '-n', '--dry-run', dest='dry_run', action='store_true', default=False,
            help="print the changes instead of committing them"
        )

        def rkb_sync_func(lib: library.Library, opts, args):
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
                        # TODO is there a clearer way other than try_sync?
                        item.try_sync(False, False)
                        pbar.update()

        rkb_sync_cmd.func = rkb_sync_func

        return [rkb_export_cmd, rkb_diff_cmd, rkb_sync_cmd]
