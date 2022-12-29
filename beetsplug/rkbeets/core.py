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


def load_beets_df(lib: Library):
    d = defaultdict(list)

    for item in lib.items():
        d['id'].append(item['id'])
        d['rating'].append(item.get('rating', default=-1))
        d['path'].append(item.path.decode('utf-8'))

    df = pandas.DataFrame(data=d)
    df['path'] = df['path'].str.normalize('NFD').str.lower()
    return df


def load_rbxml_df(xml_filename):
    '''Filtered using the beets music directory, anything outside of that is not
    considered.'''
    music_directory = global_config['directory'].get()

    xml = RekordboxXml(xml_filename)
    d = defaultdict(list)
    for t in xml.get_tracks():
        if t['Location'].lower().startswith(music_directory[1:].lower()):
            for attr in t.ATTRIBS:
                d[attr].append(t[attr])
    df = pandas.DataFrame(data=d)
    df['Location'] = ('/' + df['Location']).str.normalize('NFD').str.lower()
    return df


def crop(df_rbxml, df_beets):
    df_rbxml = df_rbxml.set_index('Location')
    df_beets = df_beets.set_index('path')

    # Crop both down to paths intersection
    df_rbxml_beets = df_rbxml.loc[df_rbxml.index.intersection(df_beets.index)]
    df_beets_rbxml = df_beets.loc[df_beets.index.intersection(df_rbxml_beets.index)]

    # Save the differences
    only_rbxml = df_rbxml.loc[df_rbxml.index.difference(df_beets.index)].index
    only_beets = df_beets.loc[df_beets.index.difference(df_rbxml_beets.index)].index

    # They are the same shape, now make the indexes match
    df_rbxml_beets.sort_index(inplace=True)
    df_beets_rbxml.sort_index(inplace=True)

    return df_beets_rbxml, df_rbxml_beets, only_rbxml, only_beets
