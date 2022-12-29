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
import pandas

from beets import config as global_config
from beets.library import Library

# pyrekordbox is chatty about missing/broken rekordbox configuration files
previous_level = logging.root.manager.disable
logging.disable(logging.CRITICAL)
try:
    from pyrekordbox.xml import RekordboxXml
finally:
    logging.disable(previous_level)


def _get_samplerate(i):
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
    'SampleRate':   _get_samplerate, # Beets is in kHz, RB in Hz
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


def load_beets_df(lib: Library):
    d = defaultdict(list)

    for item in lib.items():
        d['id'].append(item['id'])
        d['rating'].append(item.get('rating', default=-1))
        d['path'].append(item.path.decode('utf-8'))

    return pandas.DataFrame(data=d)


def load_rbxml_df(xml_filename):
    """Filtered using the beets music directory, anything outside of that is not
    considered."""
    music_directory = global_config['directory'].get()

    xml = RekordboxXml(xml_filename)
    d = defaultdict(list)
    for t in xml.get_tracks():
        if t['Location'].lower().startswith(music_directory[1:].lower()):
            for attr in t.ATTRIBS:
                d[attr].append(t[attr])
    df = pandas.DataFrame(data=d)
    df['Location'] = '/' + df['Location']
    return df


def normalize_lower_paths(series):
    return series.str.normalize('NFD').str.lower()


def crop(df_rbxml, df_beets):
    df_rbxml_index = normalize_lower_paths(df_rbxml['Location'])
    df_rbxml = df_rbxml.set_index(df_rbxml_index)

    df_beets_index = normalize_lower_paths(df_beets['path'])
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


def new_outxml():
    return RekordboxXml(name='rekordbox', version='5.4.3', company='Pioneer DJ')
