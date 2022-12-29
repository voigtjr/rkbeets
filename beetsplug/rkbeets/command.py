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

from dataclasses import dataclass
from datetime import datetime
import os

from beets.dbcore import types
from beets.library import Library
from beets.ui import Subcommand, SubcommandsOptionParser, UserError
from confuse import Subview

from beetsplug.rkbeets import core


@dataclass
class RkbeetsConfig:
    """Resolved configuration settings for operation of this plugin."""
    xml_filename: str
    xml_outfile: str


class HelpSubcommand(Subcommand):
    def __init__(self):
        super(HelpSubcommand, self).__init__(
            name='help',
            help='help TODO'
        )
    
    def func(self, *_):
        print(self.root_parser.format_help())


class ReportSubcommand(Subcommand):
    parser: SubcommandsOptionParser

    def __init__(self):
        self.parser = SubcommandsOptionParser(
            usage='beet report'
        )

        super(ReportSubcommand, self).__init__(
            name='report',
            parser=self.parser,
            help='Show information about the Rekordbox and beets libraries'
        )
    
    def func(self, rkbcfg, lib: Library, opts, args):
        if not os.path.exists(rkbcfg.xml_filename):
            raise UserError('rekordbox xml is required')

        print("Loading Rekordbox XML {}...".format(rkbcfg.xml_filename))
        df_rbxml = core.load_rbxml_df(rkbcfg.xml_filename)

        print("Loading beets library...")
        df_beets = core.load_beets_df(lib)

        _, df_rbxml_beets, only_rbxml, only_beets = core.crop(
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


class SyncSubcommand(Subcommand):
    def __init__(self):
        super(SyncSubcommand, self).__init__(
            name='sync',
            help='sync metadata from rekordbox xml to beets database. Rating is the only implemented field so far.'
        )
    
    def func(self, rkbcfg, lib: Library, opts, args):
        if not os.path.exists(rkbcfg.xml_filename):
            raise UserError('rekordbox xml is required')

        print("Loading Rekordbox XML {}...".format(rkbcfg.xml_filename))
        df_rbxml = core.load_rbxml_df(rkbcfg.xml_filename)

        print("Loading beets library...")
        df_beets = core.load_beets_df(lib)

        df_beets_rbxml, df_rbxml_beets, _, _ = core.crop(
            df_rbxml=df_rbxml, df_beets=df_beets
        )

        # Copy the rating column over
        df_beets_rbxml['new_rating'] = df_rbxml_beets['Rating']

        df_changed_ratings = df_beets_rbxml[
            df_beets_rbxml['rating'] != df_beets_rbxml['new_rating']
            ].set_index('id')

        if df_changed_ratings.empty:
            print("Nothing to update.")
            return

        for id, new_rating in df_changed_ratings['new_rating'].items():
            item = lib.get_item(id)
            print("Updating rating from {} to {} on {}".format(
                item.rating, new_rating, item))
            item.update({'rating': new_rating})
            # TODO untested
            item.store(['rating'])


class MakeImportSubcommand(Subcommand):
    def __init__(self):
        super(MakeImportSubcommand, self).__init__(
            name='make-import',
            help='make-import TODO'
        )
    
    def func(self, rkbcfg, lib: Library, opts, args):
        if rkbcfg.xml_outfile is None or not os.path.exists(rkbcfg.xml_filename):
            raise UserError('rekordbox xml is required')

        if rkbcfg.xml_outfile is None:
            raise UserError('xml output file is required for this command')

        outxml = core.new_outxml()

        print("Loading Rekordbox XML {}...".format(rkbcfg.xml_filename))
        df_rbxml = core.load_rbxml_df(rkbcfg.xml_filename)

        print("Loading beets library...")
        df_beets = core.load_beets_df(lib)

        _, df_rbxml_beets, only_rbxml, only_beets = core.crop(
            df_rbxml=df_rbxml, df_beets=df_beets
        )

        df_beets.set_index('path', inplace=True)
        for path, row in df_beets.loc[only_beets].iterrows():
            # TODO figure out why the int boxing is required
            item = lib.get_item(int(row.id))

            # Strip leading slash because rekordbox doesn't like it
            track = outxml.add_track(location=path[1:])

            for rb_field, beets_getter in core.FIELDS_TO_RB.items():
                value = beets_getter(item)
                if value is not None:
                    track[rb_field] = value
            
        print("Saving to {}".format(rkbcfg.xml_outfile))
        outxml.save(path=rkbcfg.xml_outfile)


class RkbeetsCommand(Subcommand):
    item_types = {'rating': types.INTEGER}
    XML_FILENAME = 'xml_filename'
    XML_OUTFILE = 'xml_outfile'

    @property
    def album_types(self):
        return {'rating': types.INTEGER}

    command_name = 'rkbeets'
    parser: SubcommandsOptionParser = None
    config: Subview = None

    def __init__(self, cfg):
        self.config = cfg
        self.config.add({
            self.XML_FILENAME: None,
            self.XML_OUTFILE: None,
        })

        self.parser = SubcommandsOptionParser(
            usage='beet {name} TODO'.format(name=self.command_name))

        self.parser.add_option(
            '-x', '--xml-file', dest=self.XML_FILENAME,
            help=u'Rekordbox xml filename to use')

        self.parser.add_option(
            '-o', '--xml-outfile', dest=self.XML_OUTFILE,
            help=u'Output file for xml-generating commands')

        self.parser.add_subcommand(HelpSubcommand())
        self.parser.add_subcommand(ReportSubcommand())
        self.parser.add_subcommand(SyncSubcommand())
        self.parser.add_subcommand(MakeImportSubcommand())

        super(RkbeetsCommand, self).__init__(
            name=self.command_name,
            parser=self.parser,
            help='Rekordbox integration utilities')

    def func(self, lib: Library, opts, args):
        rkbcfg = RkbeetsConfig(
            xml_filename=self.resolve(opts.xml_filename, self.XML_FILENAME),
            xml_outfile=self.resolve(opts.xml_outfile, self.XML_OUTFILE))

        subcommand, suboptions, subargs = self.parser.parse_subcommand(args)
        subcommand.func(rkbcfg, lib, suboptions, subargs)

    def resolve(self, opt, key):
        return opt if opt else self.config[key].get()
