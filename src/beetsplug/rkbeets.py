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
import copy
from functools import cached_property, reduce
from importlib import resources
import logging
import operator
from pathlib import Path

from beets import config
from beets import plugins
from beets.dbcore import types
from beets import ui
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

def format_to_kind(value):
    if value == 'AAC':
        return 'M4A File'
    if value == 'MP3':
        return 'MP3 File'
    if value == 'WAV':
        return 'WAV File'
    return value

class DimensionsDB():
    df: pandas.DataFrame

    def __init__(self):
        with resources.open_text(beetsplug, 'rkbeets-fields.csv') as f:
            self.df = pandas.read_csv(f)
    
    def to_pickle(self, dir):
        self.df.to_pickle(dir / Path('ddb.pkl'))
    
    def num_rkb_cols(self):
        cols = self.df[['rkb_field', 'rkb_type']].dropna()
        return cols.index.size

    def iterrkbcols(self):
        cols = self.df[['rkb_field', 'rkb_type']].dropna()
        return cols.itertuples(index=False)

    def num_beets_cols(self):
        cols = self.df[['beets_field', 'beets_type']].dropna()
        return cols.index.size

    def iterbeetscols(self):
        cols = self.df[['beets_field', 'beets_type']].dropna()
        return cols.itertuples(index=False)

    def get_export_conversion_info(self):
        no_export = self.df['no_export'].fillna(False)
        df_export = self.df[~no_export][['beets_field', 'rkb_field']].dropna()
        drop = self.df[no_export]['beets_field']
        return drop, df_export.itertuples(index=False)

    def get_transform_functions(self):
        def get_transform(v):
            # Could be more clever here idk. There will be more transforms...
            if v == 'format_to_kind':
                return format_to_kind
            raise RuntimeError('unknown transform function {}'.format(v))

        return (
            (row.rkb_field, get_transform(row.convert_to_rkb))
            for row in self.df[['rkb_field', 'convert_to_rkb']].dropna().itertuples(index=False)
        )
    
    def get_sync_pairs(self):
        df = self.df[self.df['beets_field'].str.startswith('rkb_').fillna(False)]
        return df[['beets_field', 'rkb_field']].itertuples(index=False)


ComputedLibraries = namedtuple('ComputedLibraries', ['df_common', 'only_beets', 'only_rbxml'])

class Libraries():
    ddb: DimensionsDB
    
    def __init__(self, lib, query, xml_path=None):
        self.ddb = DimensionsDB()
        self.items = lib.items(query)
        self.xml_path = xml_path

    @cached_property
    def df_beets(self) -> pandas.DataFrame:
        print("Loading beets library metadata...")
        with tqdm(total=self.ddb.num_beets_cols(), unit='columns') as pbar:
            def get_series(cols):
                series = pandas.Series(
                    data=[i.get(cols.beets_field) for i in self.items],
                    dtype=cols.beets_type
                )
                pbar.update()
                return series

            series_data = {
                cols.beets_field: get_series(cols)
                for cols in self.ddb.iterbeetscols()
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
                    data=[t[cols.rkb_field] for t in tracks],
                    dtype=cols.rkb_type
                )
                pbar.update()
                return series

            series_data = {
                cols.rkb_field: get_series(cols)
                for cols in self.ddb.iterrkbcols()
            }

        df = pandas.DataFrame(data=series_data)

        # Prepend a slash to the paths, Rekordbox removes this
        df['Location'] = '/' + df['Location']

        index = df['Location'].str.normalize('NFD').str.lower()
        return df.set_index(index)

    def to_pickle(self, dir):
        self.df_beets.to_pickle(dir / Path('df_beets.pkl'))
        self.ddb.to_pickle(dir)
        if self.df_rbxml is not None:
            self.df_rbxml.to_pickle(dir / Path('df_rbxml.pkl'))
        
        # TODO
        # if self.df_common is not None:
        #     self.df_common.to_pickle(dir / Path('df_common.pkl'))
        # if self.only_beets is not None:
        #     pandas.Series(self.only_beets).to_pickle(dir / Path('only_beets.pkl'))
        # if self.only_rbxml is not None:
        #     pandas.Series(self.only_rbxml).to_pickle(dir / Path('only_rbxml.pkl'))

    def crop(self, music_directory=None):
        df_r = self.df_rbxml
        if self.df_rbxml is not None and music_directory:
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

    def get_export_df(self, index=None):
        df_beets = self.df_beets if index is None else self.df_beets[index]

        drop, export_tuples = self.ddb.get_export_conversion_info()
        df = df_beets.drop(columns=drop).rename(columns={
            row.beets_field: row.rkb_field
            for row in export_tuples
        }, errors='raise')

        # Use the type's default value to fill the nulls
        for field, value in df.dtypes.items():
            if value.type() is None:
                continue
            df[field] = df[field].fillna(value=value.type())

        # Required conversions
        for field, t in self.ddb.get_transform_functions():
            df[field] = df[field].transform(t)

        return df

    def get_sync_changed(self, df_common):
        def ne(l, r):
            return l.fillna(l.dtype.type()) != r

        compares = (
            ne(df_common[cols.beets_field], df_common[cols.rkb_field])
            for cols in self.ddb.get_sync_pairs()
        )
        mask = reduce(operator.or_, compares)
        df_changed = df_common[mask]
        df_changed = df_changed.set_index('id')

        def transform_column(cols):
            default = df_common[cols.beets_field].dtype.type()
            return df_changed[cols.rkb_field].fillna(default)

        return pandas.DataFrame(data={
            cols.beets_field: transform_column(cols) 
            for cols in self.ddb.get_sync_pairs()
        }) 


def export_df(xml_path, df):
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
    # Flexible attribute typing
    item_types = {
        'rkb_Rating': types.INTEGER,
        'rkb_TrackID': types.INTEGER,
        'rkb_DateAdded': types.STRING,
        'rkb_PlayCount': types.INTEGER,
        'rkb_Mix': types.STRING,
    }

    libs: Libraries = None

    def __init__(self):
        super().__init__()

        self.config.add({
            'export_file': None,
            'rekordbox_file': None,
        })

    def check_export_file(self):
        if not self.config['export_file']:
            raise ui.UserError('export_file required')

        xml_path = Path(self.config['export_file'].get()).resolve()
        if xml_path.is_dir():
            raise ui.UserError("xml_outdir is a directory: {}".format(xml_path))
        return xml_path

    def commands(self):
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

        def rkb_export_func(lib, opts, args):
            self.config.set_args(opts)
            export_path = self.check_export_file()

            self.libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            index = None
            if opts.missing:
                cl = self.libs.crop(config['directory'].get())

                if cl.only_beets.empty:
                    print("nothing to do: no tracks are missing from rekordbox")
                    return

                index = cl.only_beets

            df_export = self.libs.get_export_df(index)

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

        def rkb_diff_func(lib, opts, args):
            self.config.set_args(opts)

            self.libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            cl = self.libs.crop(config['directory'].get())

            if opts.pickle:
                print("Writing dataframes to {}".format(opts.pickle))
                self.libs.to_pickle(opts.pickle)

            print("{:>6d} tracks in rekordbox library (in beets directory)".format(
                cl.df_common.index.size + cl.only_rbxml.size))
            print("{:>6d} tracks in beets library".format(self.libs.df_beets.index.size))
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

        def rkb_sync_func(lib, opts, args):
            self.config.set_args(opts)

            self.libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            cl = self.libs.crop(config['directory'].get())

            df_sync_changed = self.libs.get_sync_changed(cl.df_common)

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
