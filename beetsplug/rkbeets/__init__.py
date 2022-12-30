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
import logging

from beets import config
from beets import plugins
from beets.dbcore import types
from beets import ui
from beets import util
import pandas

# pyrekordbox is chatty about missing/broken rekordbox configuration files
previous_level = logging.root.manager.disable
logging.disable(logging.CRITICAL)
try:
    import pyrekordbox.xml as pxml
finally:
    logging.disable(previous_level)


def get_samplerate(i):
    value = i.get('samplerate')
    return None if value is None else value / 1000


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

    def load_rbxml_df(self, xml_filename):
        """Filtered using the beets music directory, anything outside of that is not
        considered."""
        music_directory = config['directory'].get()

        xml = pxml.RekordboxXml(xml_filename)
        d = defaultdict(list)
        for t in xml.get_tracks():
            if t['Location'].lower().startswith(music_directory[1:].lower()):
                for attr in t.ATTRIBS:
                    d[attr].append(t[attr])
        df = pandas.DataFrame(data=d)
        df['Location'] = '/' + df['Location']
        return df

    def load_beets_df(self, lib):
        d = defaultdict(list)

        for item in lib.items():
            d['id'].append(item['id'])
            d['rating'].append(item.get('rating', default=-1))
            d['path'].append(item.path.decode('utf-8'))

        return pandas.DataFrame(data=d)

    def crop(self, df_rbxml, df_beets):
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

    def commands(self):
        rkb_report_cmd = ui.Subcommand(
            'rkb-report', help='show information about the rekordbox and beets libraries'
        )
        rkb_report_cmd.parser.add_option(
            '-x', '--xml-filename', dest='xml_filename',
            help=u'xml file exported from rekordbox'
        )

        def rkb_report_func(lib, opts, args):
            self.config.set_args(opts)

            print("loading '{}'...".format(self.config['xml_filename'].get()))
            df_rbxml = self.load_rbxml_df(self.config['xml_filename'].get())

            print("loading beets library...")
            df_beets = self.load_beets_df(lib)

            _, df_rbxml_beets, only_rbxml, only_beets = self.crop(
                df_rbxml=df_rbxml, df_beets=df_beets
            )

            print("Rekordbox has {} tracks in the beets music directory".format(
                df_rbxml.index.size))
            print("Beets has {} tracks".format(df_beets.index.size))
            print("They share {} tracks".format(df_rbxml_beets.index.size))

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

            print("loading '{}'...".format(self.config['xml_filename'].get()))
            df_rbxml = self.load_rbxml_df(self.config['xml_filename'].get())

            print("loading beets library...")
            df_beets = self.load_beets_df(lib)

            df_beets_rbxml, df_rbxml_beets, _, _ = self.crop(
                df_rbxml=df_rbxml, df_beets=df_beets
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
            'rkb-make-import', help='sync metadata from rekordbox xml to beets database'
        )

        rkb_make_import_cmd.parser.add_option(
            '-x', '--xml-filename', dest='xml_filename',
            help=u'xml file exported from rekordbox'
        )

        rkb_make_import_cmd.parser.add_option(
            '-o', '--xml-outfile', dest='xml_outfile',
            help=u'file where beets will write xml for import into rekordbox'
        )

        def rkb_make_import_func(lib, opts, args):
            self.config.set_args(opts)
            if not self.config['xml_outfile'].get():
                raise ui.UserError('xml_outfile required')

            print("Loading Rekordbox XML {}...".format(self.config['xml_filename'].get()))
            df_rbxml = self.load_rbxml_df(self.config['xml_filename'].get())

            print("loading beets library...")
            df_beets = self.load_beets_df(lib)

            _, _, _, only_beets = self.crop(
                df_rbxml=df_rbxml, df_beets=df_beets
            )

            if only_beets.empty:
                print("nothing to do: no tracks are missing from rekordbox")
                return

            outxml = pxml.RekordboxXml(
                name='rekordbox', version='5.4.3', company='Pioneer DJ'
            )
            df_beets.set_index('path', inplace=True)
            for path, row in df_beets.loc[only_beets].iterrows():
                # TODO figure out why the int boxing is required
                item = lib.get_item(int(row.id))

                # Strip leading slash because rekordbox doesn't like it
                track = outxml.add_track(location=path[1:])

                for rb_field, beets_getter in self.FIELDS_TO_RB.items():
                    value = beets_getter(item)
                    if value is not None:
                        track[rb_field] = value
                
            print("Saving to {}".format(self.config['xml_outfile'].get()))
            outxml.save(path=self.config['xml_outfile'].get())

        rkb_make_import_cmd.func = rkb_make_import_func

        return [rkb_report_cmd, rkb_sync_cmd, rkb_make_import_cmd]

