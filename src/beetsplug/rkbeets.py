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

from collections import defaultdict
import copy
from dataclasses import dataclass
import glob
from importlib import resources
import logging
from pathlib import Path
import re

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

    def transform_for_export(self, df_in):
        no_export = self.df['no_export'].fillna(False)
        df_export = self.df[~no_export][['beets_field', 'rkb_field']].dropna()
        drop = self.df[no_export]['beets_field']
        return df_in.drop(columns=drop).rename(columns={
            row.beets_field: row.rkb_field
            for row in df_export.itertuples(index=False)
        }, errors='raise')

    def get_export_drop(self):
        return 

    def get_fills(self):
        def value(t):
            if t == 'string':
                return ''
            return 0

        return {
            row.rkb_field: value(row.rkb_type)
            for row in self.df[['rkb_field', 'rkb_type']].dropna().itertuples(index=False)
        }
        
    def transform_tuples(self):
        def get_transform(v):
            # Could be more clever here idk. There will be more transforms...
            if v == 'format_to_kind':
                return format_to_kind
            raise RuntimeError('unknown transform function {}'.format(v))

        return (
            (row.rkb_field, get_transform(row.convert_to_rkb))
            for row in self.df[['rkb_field', 'convert_to_rkb']].dropna().itertuples(index=False)
        )

class Libraries():
    ddb: DimensionsDB
    df_beets: pandas.DataFrame
    df_rbxml: pandas.DataFrame = None
    df_common: pandas.DataFrame = None
    only_beets: pandas.Series = None
    only_rbxml: pandas.Series = None
    
    def __init__(self, lib, query, xml_path=None):
        self.ddb = DimensionsDB()
        self.df_beets = self.load_beets_df(lib.items(query))
        self.df_rbxml = self.load_rkb_library(xml_path)

    def load_beets_df(self, items):
        print("Loading beets library metadata...")
        with tqdm(total=self.ddb.num_beets_cols(), unit='columns') as pbar:
            def get_series(cols):
                series = pandas.Series(
                    data=[i.get(cols.beets_field) for i in items],
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

    def load_rkb_library(self, xml_path):
        try:
            return self.load_rbxml_df(xml_path)
        except BaseException as e:
            # TODO be more specific
            breakpoint()
            print(e)

        return None
    
    def load_rbxml_df(self, xml_path):
        xml = pxml.RekordboxXml(xml_path)
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

        # TODO report on normalization errors? definitely on inconsistent spaces
        index = df['Location'].str.normalize('NFD').str.lower()
        return df.set_index(index)

    def to_pickle(self, dir):
        self.df_beets.to_pickle(dir / Path('df_beets.pkl'))
        if self.df_rbxml is not None:
            self.df_rbxml.to_pickle(dir / Path('df_rbxml.pkl'))
        if self.df_common is not None:
            self.df_common.to_pickle(dir / Path('df_common.pkl'))
        if self.only_beets is not None:
            self.only_beets.to_pickle(dir / Path('only_beets.pkl'))
        if self.only_rbxml is not None:
            self.only_rbxml.to_pickle(dir / Path('only_rbxml.pkl'))

    def crop(self, music_directory=None):
        df_r = self.df_rbxml
        if self.df_rbxml is not None and music_directory:
            # Filter tracks outside of music directory
            i = self.df_rbxml.index.str.startswith(music_directory.lower())
            df_r = self.df_rbxml[i]

        libs = copy.deepcopy(self)

        libs.only_rbxml = df_r.index.difference(self.df_beets.index)
        libs.only_beets = self.df_beets.index.difference(df_r.index)

        intersection = df_r.index.intersection(self.df_beets.index)
        libs.df_common = self.df_beets.loc[intersection].join(
            df_r.loc[intersection]
        )

        return libs

    def get_export_df(self, index=None):
        df_beets = self.df_beets if index is None else self.df_beets[index]
        df = self.ddb.transform_for_export(df_beets)

        # Fill nulls for import
        df = df.fillna(self.ddb.get_fills())

        # Required conversions
        for field, t in self.ddb.transform_tuples():
            df[field] = df[field].transform(t)

        return df

    def export_df(self, xml_path, df):
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
                # outxml.add_track(location=location[1:], **row._asdict())
                outxml.add_track(
                    location=location,
                    # Filter empty strings
                    **{
                        k: v for k, v in row._asdict().items() if v != ''
                    }
                )
                pbar.update()

        print("Writing {}...".format(xml_path))
        outxml.save(path=xml_path)


class RkBeetsPlugin(plugins.BeetsPlugin):
    # Flexible attribute typing
    item_types = {
        'rkb-Rating': types.INTEGER,
        'rkb-TrackID': types.INTEGER,
        'rkb-DateAdded': types.STRING,
        'rkb-PlayCount': types.INTEGER,
        'rkb-Mix': types.STRING,
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
        # TODO address self.libs. direct access

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
            # TODO use stdout?
            export_path = self.check_export_file()

            self.libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            index = None
            if opts.missing:
                self.libs = self.libs.crop(config['directory'].get())

                if self.libs.only_beets.empty:
                    print("nothing to do: no tracks are missing from rekordbox")
                    return

                index = self.libs.only_beets

            df_export = self.libs.get_export_df(index)

            self.libs.export_df(export_path, df_export)

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

            self.libs = self.libs.crop(config['directory'].get())

            if opts.pickle:
                print("Writing dataframes to {}".format(opts.pickle))
                self.libs.to_pickle(opts.pickle)

            print("{:>6d} tracks in rekordbox library (in beets directory)".format(
                self.libs.df_common.index.size + self.libs.only_rbxml.size))
            print("{:>6d} tracks in beets library".format(self.libs.df_beets.index.size))
            print("{:>6d} shared tracks in both".format(self.libs.df_common.index.size))

            if not self.libs.only_rbxml.empty:
                print("Only in Rekordbox:")
                for path in self.libs.only_rbxml:
                    print("    ", path)

            if not self.libs.only_beets.empty:
                print("Only in beets:")
                for path in self.libs.only_beets:
                    print("    ", path)

        rkb_diff_cmd.func = rkb_diff_func

        rkb_sync_cmd = ui.Subcommand(
            'rkb-sync', help='sync metadata from rekordbox xml to beets database'
        )
        rkb_sync_cmd.parser.add_option(
            '-r', '--rekordbox-file', dest='rekordbox_file',
            help="rekordbox xml library"
        )

        def rkb_sync_func(lib, opts, args):
            self.config.set_args(opts)

            self.libs = Libraries(
                lib, query=ui.decargs(args),
                xml_path = self.config['rekordbox_file'].get()
            )

            self.libs = self.libs.crop(config['directory'].get())

            # TODO use a new column indicating sync
            df_changed_ratings = self.libs.df_common[
                (self.libs.df_common['rkb-Rating'] != self.libs.df_common['Rating'])
                | (self.libs.df_common['rkb-TrackID'] != self.libs.df_common['TrackID'])
                | (self.libs.df_common['rkb-DateAdded'] != self.libs.df_common['DateAdded'])
                | (self.libs.df_common['rkb-PlayCount'] != self.libs.df_common['PlayCount'])
                | (self.libs.df_common['remixer'] != self.libs.df_common['Remixer'])
                | (self.libs.df_common['rkb-Mix'] != self.libs.df_common['Mix'])
            ].set_index('id')

            if df_changed_ratings.empty:
                print("nothing to update")
                return

            print("Updating {} tracks...".format(df_changed_ratings.index.size))
            with tqdm(total=df_changed_ratings.index.size, unit='tracks') as pbar:
                # TODO change to itertuples
                for id, row in df_changed_ratings.iterrows():
                    item = lib.get_item(id)
                    item.update({
                        'rkb-Rating': row['Rating'],
                        'rkb-TrackID': row['TrackID'],
                        'rkb-DateAdded': row['DateAdded'],
                        'rkb-PlayCount': row['PlayCount'],
                        'remixer': row['Remixer'],
                        'rkb-Mix': row['Mix'],
                    })
                    # TODO there has to be a more direct way
                    # item.try_sync(False, False)
                    pbar.update()

        rkb_sync_cmd.func = rkb_sync_func

        return [rkb_export_cmd, rkb_diff_cmd, rkb_sync_cmd]
