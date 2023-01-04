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

from collections import defaultdict, namedtuple
from dataclasses import dataclass
import logging
from pathlib import Path

from beets import config
from beets import plugins
from beets.dbcore import types
from beets import ui
import pandas
from tqdm import tqdm

VERSION = '0.1.0'

# pyrekordbox is chatty about missing/broken rekordbox configuration files
previous_level = logging.root.manager.disable
logging.disable(logging.CRITICAL)
try:
    import pyrekordbox.xml as pxml # type: ignore
finally:
    logging.disable(previous_level)

LibraryDataFrames = namedtuple('LibraryDataFrames', ['rbxml', 'beets'])

def format_to_kind(value):
    if value == 'AAC':
        return 'M4A File'
    if value == 'MP3':
        return 'MP3 File'
    if value == 'WAV':
        return 'WAV File'
    return value

def crop(dfs: LibraryDataFrames):
    # Filter tracks outside of music directory
    music_directory = config['directory'].get()
    rbxml_in_beets_dir = dfs.rbxml.index.str.startswith(music_directory.lower())
    df_rbxml = dfs.rbxml[rbxml_in_beets_dir]

    # Crop both down to paths intersection
    df_rbxml_beets = df_rbxml.loc[df_rbxml.index.intersection(dfs.beets.index)]
    df_beets_rbxml = dfs.beets.loc[dfs.beets.index.intersection(df_rbxml_beets.index)]

    # Save the differences
    only_rbxml = df_rbxml.index.difference(dfs.beets.index)
    only_beets = dfs.beets.index.difference(df_rbxml_beets.index)

    # Make them the same
    df_beets_rbxml.sort_index(inplace=True)
    df_rbxml_beets.sort_index(inplace=True)
    df_rbxml_beets.set_index(df_beets_rbxml.index, inplace=True)
    df_common = df_beets_rbxml.join(df_rbxml_beets)

    return df_common, only_rbxml, only_beets

def load_rbxml(xml_path):
    # Types from https://cdn.rekordbox.com/files/20200410160904/xml_format_list.pdf
    # TODO combine this list and the other two in this file into one dataframe-backed data structure
    COLUMNS = {
        'Album': 'string',
        'Artist': 'string',
        'AverageBpm': 'float64',
        'BitRate': 'int32',
        'Colour': 'string',
        'Comments': 'string',
        'Composer': 'string',
        'DateAdded': 'string',
        'DateModified': 'string',
        'DiscNumber': 'int32',
        'Genre': 'string',
        'Grouping': 'string',
        'Kind': 'string',
        'Label': 'string',
        'LastPlayed': 'string',
        'Location': 'string',
        'Mix': 'string',
        'Name': 'string',
        'PlayCount': 'int32',
        'Rating': 'int32',
        'Remixer': 'string',
        'SampleRate': 'float64',
        'Size': 'int64',
        'Tonality': 'string',
        'TotalTime': 'float64',
        'TrackID': 'int64',
        'TrackNumber': 'int32',
        'Year': 'int32',
    }

    xml = pxml.RekordboxXml(xml_path)
    d = defaultdict(list)
    tracks = xml.get_tracks()
    with tqdm(total=len(tracks), unit='tracks') as pbar:
        for t in xml.get_tracks():
            for col, dtype in COLUMNS.items():
                d[(col, dtype)].append(t[col])
            pbar.update()
    return d

def load_rbxml_df(xml_data):
    data = {
        col: pandas.Series(values, dtype=dtype)
        for (col, dtype), values in xml_data.items()
    }

    df = pandas.DataFrame(data=data)

    # Prepend a slash to the paths, Rekordbox removes this
    df['Location'] = '/' + df['Location']

    # TODO report on normalization errors? definitely on inconsistent spaces
    index = df['Location'].str.normalize('NFD').str.lower()
    return df.set_index(index)

def load_beets_df(items):
    # beets field, dtype
    COLUMNS = [
        ('album', 'string'),
        ('artist', 'string'),
        ('bitrate', 'int32'),
        ('comments', 'string'),
        ('composer', 'string'),
        ('disc', 'int32'),
        ('filesize', 'int64'),
        ('format', 'string'),
        ('genre', 'string'),
        ('grouping', 'string'),
        ('id', 'int32'),
        ('label', 'string'),
        ('length', 'int32'),
        ('path', 'bytes'),
        ('remixer', 'string'),
        ('samplerate', 'int32'),
        ('title', 'string'),
        ('track', 'int32'),
        ('year', 'int32'),

        # Rekordbox-specific fields are prefixed and can be null (e.g. Int32)
        ('rkb-DateAdded', 'string'),
        ('rkb-Mix', 'string'),
        ('rkb-PlayCount', 'Int32'),
        ('rkb-Rating', 'Int32'),
        ('rkb-TrackID', 'Int64'),
    ]

    series_data = {
        field: pandas.Series([i.get(field) for i in items], dtype=dtype)
        for field, dtype in COLUMNS 
    }
    df = pandas.DataFrame(data=series_data)
    index = df['path'].str.decode('utf-8').str.normalize('NFD').str.lower()
    return df.set_index(index)

def get_export_df(df_beets):
    df = df_beets.rename(columns={
        'album': 'Album',
        'artist': 'Artist',
        # 'AverageBpm'
        'bitrate': 'BitRate',
        # 'Colour': None,
        'comments': 'Comments',
        'composer': 'Composer',
        'rkb-DateAdded': 'DateAdded',
        # 'DateModified'
        'disc': 'DiscNumber',
        'genre': 'Genre',
        'grouping': 'Grouping',
        'format': 'Kind',
        'label': 'Label',
        # 'LastPlayed'
        'rkb-Mix': 'Mix',
        'title': 'Name',
        'rkb-PlayCount': 'PlayCount',
        'rkb-Rating': 'Rating',
        'remixer': 'Remixer',
        'samplerate': 'SampleRate',
        'filesize': 'Size',
        # 'Tonality'
        'length': 'TotalTime',
        # 'rkb-TrackID': 'TrackID', # read only
        'track': 'TrackNumber',
        'year': 'Year',
    }, errors='raise').drop(columns=['id', 'path', 'rkb-TrackID'])
 
    # Fill nulls for import
    df[['DateAdded', 'Mix']] = df[['DateAdded', 'Mix']].fillna('')
    df[['PlayCount', 'Rating']] = df[['PlayCount', 'Rating']].fillna(0)

    # Required conversions
    df['SampleRate'] = df['SampleRate'].transform(lambda v: v * 1000.0)
    df['Kind'] = df['Kind'].transform(format_to_kind)

    return df

def export_df(xml_path, df):
    outxml = pxml.RekordboxXml(
        name='rekordbox', version='5.4.3', company='Pioneer DJ'
    )

    with tqdm(total=df.index.size, unit='tracks') as pbar:
        for row in df.itertuples():
            # We need the Index (path/Location) but it can't stay in the dict
            data = row._asdict()
            
            # TODO this is incorrectly-cased (it's lower()ed)
            path = data.pop('Index')

            # Strip leading slash because rekordbox doesn't like it
            outxml.add_track(location=path[1:], **data)
            pbar.update()

    print("Writing to {}...".format(xml_path))
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

    def __init__(self):
        super().__init__()

        self.config.add({
            'export_file': None,
            'rekordbox_file': None,
        })

    def load_rkb_library(self, required=True):
        try:
            if not self.config['rekordbox_file']:
                raise ui.UserError("rekordbox_file required")

            xml_path = Path(self.config['rekordbox_file'].get()).resolve()
            if not xml_path.exists():
                raise ui.UserError("rekordbox_file doesn't exist: {}".format(xml_path))
            
            if not xml_path.is_file():
                raise ui.UserError("rekordbox_file not a file: {}".format(xml_path))

            print("loading '{}'...".format(xml_path))
            xml_data = load_rbxml(xml_path)
            return load_rbxml_df(xml_data)

        except ui.UserError:
            if required:
                raise

        return None

    def load_libraries(self, lib=None, query=None, rkb_required=True):
        df_rbxml = self.load_rkb_library(required=rkb_required)

        if lib is not None:
            print("loading beets library...")
            df_beets = load_beets_df(lib.items(query))
            return LibraryDataFrames(df_rbxml, df_beets)

        return LibraryDataFrames(df_rbxml, None)
    
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
            xml_path = self.check_export_file()

            dfs = self.load_libraries(
                lib, query=ui.decargs(args), rkb_required=opts.missing
            )
            df_beets = dfs.beets

            if opts.missing:
                _, _, only_beets = crop(dfs)

                if only_beets.empty:
                    print("nothing to do: no tracks are missing from rekordbox")
                    return

                df_beets = dfs.beets.loc[only_beets]
            
            export_df(xml_path, get_export_df(dfs.beets))

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

            dfs = self.load_libraries(lib)

            if opts.pickle:
                print("Writing dataframes to {}".format(opts.pickle))
                dfs.beets.to_pickle(opts.pickle / Path('beets.pkl'))
                dfs.rbxml.to_pickle(opts.pickle / Path('rbxml.pkl'))

            df_common, only_rbxml, only_beets = crop(dfs)

            if opts.pickle:
                df_common.to_pickle(opts.pickle / Path('df_common.pkl'))

            print("{:>6d} tracks in rekordbox library (in beets directory)".format(
                df_common.index.size + only_rbxml.size))
            print("{:>6d} tracks in beets library".format(dfs.beets.index.size))
            print("{:>6d} shared tracks in both".format(df_common.index.size))

            if not only_rbxml.empty:
                print("Only in Rekordbox:")
                for path in only_rbxml:
                    print("    ", path)

            if not only_beets.empty:
                print("Only in beets:")
                for path in only_beets:
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
            dfs = self.load_libraries(lib, query=ui.decargs(args))

            df_common, _, _ = crop(dfs)
            
            df_changed_ratings = df_common[
                (df_common['rkb-Rating'] != df_common['Rating'])
                | (df_common['rkb-TrackID'] != df_common['TrackID'])
                | (df_common['rkb-DateAdded'] != df_common['DateAdded'])
                | (df_common['rkb-PlayCount'] != df_common['PlayCount'])
                | (df_common['remixer'] != df_common['Remixer'])
                | (df_common['rkb-Mix'] != df_common['Mix'])
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
                    item.try_sync(False, False)
                    pbar.update()

        rkb_sync_cmd.func = rkb_sync_func

        return [rkb_export_cmd, rkb_diff_cmd, rkb_sync_cmd]
