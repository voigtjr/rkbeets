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
from dataclasses import dataclass
import logging
from pathlib import Path

from beets import config
from beets import plugins
from beets.dbcore import types
from beets import ui
from beets import util
import pandas
from tqdm import tqdm

# pyrekordbox is chatty about missing/broken rekordbox configuration files
previous_level = logging.root.manager.disable
logging.disable(logging.CRITICAL)
try:
    import pyrekordbox.xml as pxml # type: ignore
finally:
    logging.disable(previous_level)


@dataclass
class LibraryDataframes:
    df_rbxml: pandas.DataFrame
    df_beets: pandas.DataFrame = None

def get_samplerate(i):
    value = i.get('samplerate')
    return None if value is None else value / 1000

FIELDS_TO_RB = {
    'Name':         lambda i: i.get('title'),
    'Artist':       lambda i: i.get('artist'),
    'Composer':     lambda i: i.get('composer'),
    'Album':        lambda i: i.get('album'),
    'Grouping':     lambda i: i.get('grouping'),
    'Genre':        lambda i: i.get('genre'),
    'Kind':         lambda i: i.get('format'),
    'Size':         lambda i: i.get('filesize'), # Item getter calls try_filesize()
    'TotalTime':    lambda i: i.get('length'),
    'DiscNumber':   lambda i: i.get('disc'),
    'TrackNumber':  lambda i: i.get('track'),
    'Year':         lambda i: i.get('year'),
    'BitRate':      lambda i: i.get('bitrate'),
    'SampleRate':   get_samplerate, # Beets is in kHz, RB in Hz
    'Comments':     lambda i: i.get('comments'),
    'Rating':       lambda i: i.get('rating'),
    'Remixer':      lambda i: i.get('remixer'),
    'Label':        lambda i: i.get('label'),
    # 'Location':     lambda i: i.get('path'), # Set in ctor
    # 'AverageBpm':   None,
    # 'DateModified': None,
    # 'DateAdded':    None,
    # 'Tonality':     None,
    # 'Mix':          None,           # TODO: capture?
    # 'Colour':       None,           # TODO: capture?
}

def crop(df_rbxml, df_beets):
    # TODO report on normalization errors? definitely on inconsistent spaces
    df_rbxml_index = df_rbxml['Location'].str.normalize('NFD').str.lower()
    df_rbxml = df_rbxml.set_index(df_rbxml_index)

    df_beets_index = df_beets['path'].str.normalize('NFD').str.lower()
    df_beets = df_beets.set_index(df_beets_index)

    # Crop both down to paths intersection
    df_rbxml_beets = df_rbxml.loc[df_rbxml.index.intersection(df_beets.index)]
    df_beets_rbxml = df_beets.loc[df_beets.index.intersection(df_rbxml_beets.index)]

    # Save the differences
    only_rbxml = df_rbxml.loc[df_rbxml.index.difference(df_beets.index)]['Location']
    only_beets = df_beets.loc[df_beets.index.difference(df_rbxml_beets.index)]['path']

    # They are the same shape, now make the indexes match
    df_rbxml_beets.sort_index(inplace=True)
    df_beets_rbxml.sort_index(inplace=True)

    return df_beets_rbxml, df_rbxml_beets, only_rbxml, only_beets

def load_rbxml_df(xml_path):
    """Filtered using the beets music directory, anything outside of that is not
    considered."""
    music_directory = config['directory'].get()

    xml = pxml.RekordboxXml(xml_path)
    d = defaultdict(list)
    tracks = xml.get_tracks()
    with tqdm(total=len(tracks), unit='tracks') as pbar:
        for t in xml.get_tracks():
            if t['Location'].lower().startswith(music_directory[1:].lower()):
                for attr in t.ATTRIBS:
                    d[attr].append(t[attr])
            pbar.update()
    df = pandas.DataFrame(data=d)
    df['Location'] = '/' + df['Location']
    return df

def load_beets_df(lib, query=None):
    d = defaultdict(list)
    items = lib.items(query)

    with tqdm(total=len(items), unit='tracks') as pbar:
        for item in items:
            d['id'].append(item['id'])
            d['rating'].append(item.get('rating', default=-1))
            d['path'].append(item.path.decode('utf-8'))
            pbar.update()

    return pandas.DataFrame(data=d)

def make_import(xml_path, lib, df_path_id):
    outxml = pxml.RekordboxXml(
        name='rekordbox', version='5.4.3', company='Pioneer DJ'
    )
    
    print("Transforming {} tracks to xml...".format(df_path_id.size))
    with tqdm(total=df_path_id.size, unit='tracks') as pbar:
        for path, id in df_path_id.items():
            # TODO figure out why the int boxing is required
            item = lib.get_item(int(id))

            # Strip leading slash because rekordbox doesn't like it
            track = outxml.add_track(location=path[1:])

            for rb_field, beets_getter in FIELDS_TO_RB.items():
                value = beets_getter(item)
                if value is not None:
                    track[rb_field] = value
            pbar.update()
        
    print("Writing to {}...".format(xml_path))
    outxml.save(path=xml_path)

class RkBeetsPlugin(plugins.BeetsPlugin):
    # Flexible attribute typing
    item_types = {'rating': types.INTEGER}

    # Flexible attribute typing
    @property
    def album_types(self):
        return {'rating': types.INTEGER}

    def __init__(self):
        super().__init__()

        self.config.add({
            'xml_filename': None,
            'xml_outfile': None,
        })

    def load_rkb_library(self, required=True):
        try:
            if not self.config['xml_filename']:
                raise ui.UserError("xml_filename required")

            xml_path = Path(self.config['xml_filename'].get()).resolve()
            if not xml_path.exists():
                raise ui.UserError("xml_filename doesn't exist: {}".format(xml_path))
            
            if not xml_path.is_file():
                raise ui.UserError("xml_filename not a file: {}".format(xml_path))

            print("loading '{}'...".format(xml_path))
            return load_rbxml_df(xml_path)

        except ui.UserError:
            if required:
                raise

        return None

    def load_libraries(self, lib=None, query=None, rkb_required=True):
        df_rbxml = self.load_rkb_library(required=rkb_required)

        if lib is not None:
            print("loading beets library...")
            df_beets = load_beets_df(lib, query)
            return LibraryDataframes(df_rbxml, df_beets)

        return LibraryDataframes(df_rbxml)
    
    def check_xml_outfile(self):
        if not self.config['xml_outfile']:
            raise ui.UserError('xml_outfile required')

        xml_path = Path(self.config['xml_outfile'].get()).resolve()
        if xml_path.is_dir():
            raise ui.UserError("xml_outdir is a directory: {}".format(xml_path))
        return xml_path

    def commands(self):
        rkb_report_cmd = ui.Subcommand(
            'rkb-report', help='show information about the rekordbox and beets libraries'
        )
        rkb_report_cmd.parser.add_option(
            '-x', '--xml-filename', dest='xml_filename',
            help=u'xml file exported from rekordbox'
        )

        rkb_report_cmd.parser.add_option(
            '-v', '--verbose', dest='verbose', action='store_true', default=False,
            help="print out paths for tracks not in the others' library (excludes files outside of beets music directory)"
        )

        def rkb_report_func(lib, opts, args):
            self.config.set_args(opts)

            dfs = self.load_libraries(lib)

            _, df_rbxml_beets, only_rbxml, only_beets = crop(
                df_rbxml=dfs.df_rbxml, df_beets=dfs.df_beets
            )

            print("{:>6d} tracks in rekordbox library (in beets directory)".format(
                dfs.df_rbxml.index.size))
            print("{:>6d} tracks in beets library".format(dfs.df_beets.index.size))
            print("{:>6d} shared tracks in both".format(df_rbxml_beets.index.size))

            if opts.verbose:
                if not only_rbxml.empty:
                    print("Only in Rekordbox:")
                    for path in only_rbxml:
                        print("    ", path)

                if not only_beets.empty:
                    print("Only in beets:")
                    for path in only_beets:
                        print("    ", path)

        rkb_report_cmd.func = rkb_report_func

        rkb_sync_cmd = ui.Subcommand(
            'rkb-sync', help='sync metadata from rekordbox xml to beets database'
        )
        rkb_sync_cmd.parser.add_option(
            '-x', '--xml-filename', dest='xml_filename',
            help=u'xml file exported from rekordbox'
        )

        def rkb_sync_func(lib, opts, args):
            self.config.set_args(opts)
            dfs = self.load_libraries(lib, query=ui.decargs(args))

            df_beets_rbxml, df_rbxml_beets, _, _ = crop(
                df_rbxml=dfs.df_rbxml, df_beets=dfs.df_beets
            )
            
            # copy the rating column over
            df_beets_rbxml['new_rating'] = df_rbxml_beets['Rating']

            df_changed_ratings = df_beets_rbxml[
                df_beets_rbxml['rating'] != df_beets_rbxml['new_rating']
                ].set_index('id')

            if df_changed_ratings.empty:
                print("nothing to update")
                return

            for id, row in df_changed_ratings.iterrows():
                item = lib.get_item(id)
                print("updating rating from {} to {} on {}".format(
                    row.rating, row.new_rating, item)
                )
                item.update({'rating': row.new_rating})
                
                # TODO there has to be a more direct way
                item.try_sync(False, False)

        rkb_sync_cmd.func = rkb_sync_func

        rkb_make_import_cmd = ui.Subcommand(
            'rkb-make-import',
            help="sync metadata from rekordbox xml to beets database"
        )

        rkb_make_import_cmd.parser.add_option(
            '-x', '--xml-filename', dest='xml_filename',
            help="xml file exported from rekordbox"
        )

        rkb_make_import_cmd.parser.add_option(
            '-o', '--xml-outfile', dest='xml_outfile',
            help="file where beets will write xml for import into rekordbox"
        )

        rkb_make_import_cmd.parser.add_option(
            '-m', '--missing', dest='missing', action='store_true', default=False,
            help="only consider files not already in rekordbox"
        )

        def rkb_make_import_func(lib, opts, args):
            self.config.set_args(opts)
            xml_path = self.check_xml_outfile()

            dfs = self.load_libraries(
                lib, query=ui.decargs(args), rkb_required=opts.missing
            )

            if opts.missing:
                _, _, _, only_beets = crop(
                    df_rbxml=dfs.df_rbxml, df_beets=dfs.df_beets,
                )

                if only_beets.empty:
                    print("nothing to do: no tracks are missing from rekordbox")
                    return

                dfs.df_beets.set_index('path', inplace=True)
                make_import(xml_path, lib, dfs.df_beets.loc[only_beets]['id'])
                return
            
            dfs.df_beets.set_index('path', inplace=True)
            make_import(xml_path, lib, dfs.df_beets['id'])

        rkb_make_import_cmd.func = rkb_make_import_func

        return [rkb_report_cmd, rkb_sync_cmd, rkb_make_import_cmd]
