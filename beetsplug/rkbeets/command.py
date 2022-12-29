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

from beets.dbcore import types
from beets.library import Library
from beets.ui import Subcommand, SubcommandsOptionParser
from confuse import Subview

from beetsplug.rkbeets import core


class HelpSubcommand(Subcommand):
    def __init__(self):
        super(HelpSubcommand, self).__init__(
            name='help',
            help='help TODO'
        )
    
    def func(self, *_):
        print(self.root_parser.format_help())


class ReportSubcommand(Subcommand):
    parser: SubcommandsOptionParser = None

    def __init__(self):
        self.parser = SubcommandsOptionParser(
            usage='beet report'
        )

        super(ReportSubcommand, self).__init__(
            name='report',
            parser=self.parser,
            help='Show information about the Rekordbox and beets libraries'
        )
    
    def func(self, xml_filename, lib: Library, opts, args):
        print("Loading Rekordbox XML {}...".format(repr(xml_filename)))
        df_rbxml = core.load_rbxml_df(xml_filename)

        print("Loading beets library...")
        df_beets = core.load_beets_df(lib)

        _, df_rbxml_beets, only_rbxml, only_beets = core.crop(
            df_rbxml=df_rbxml, df_beets=df_beets
        )

        print("Rekordbox has {} tracks in the beets music directory".format(df_rbxml.index.size))
        print("Beets has {} tracks".format(df_beets.index.size))
        print("They share {} tracks".format(df_rbxml_beets.index.size))

        if not only_rbxml.empty:
            print("Only in Rekordbox:")
            for path in only_rbxml:
                print("    ", path)

        if not only_beets.empty:
            print("Only in Rekordbox:")
            for path in only_beets:
                print("    ", path)


class SyncSubcommand(Subcommand):
    def __init__(self):
        super(SyncSubcommand, self).__init__(
            name='sync',
            help='sync metadata from rekordbox xml to beets database. Rating is the only implemented field so far.'
        )
    
    def func(self, xml_filename, lib: Library, opts, args):
        print("Loading Rekordbox XML {}...".format(repr(xml_filename)))
        df_rbxml = core.load_rbxml_df(xml_filename)

        print("Loading beets library...")
        df_beets = core.load_beets_df(lib)

        df_beets_rbxml, df_rbxml_beets, _, _ = core.crop(
            df_rbxml=df_rbxml, df_beets=df_beets
        )

        # Copy the rating column over
        df_beets_rbxml['new_rating'] = df_rbxml_beets['Rating']

        df_changed_ratings = df_beets_rbxml[df_beets_rbxml['rating'] != df_beets_rbxml['new_rating']].set_index('id')

        if df_changed_ratings.empty:
            print("Nothing to update.")
            return

        for id, new_rating in df_changed_ratings['new_rating'].items():
            item = lib.get_item(id)
            print("Updating rating from {} to {} on {}".format(item.rating, new_rating, item))
            item.update({'rating': new_rating})
            item.try_sync(False, False)


class MakeImportSubcommand(Subcommand):
    def __init__(self):
        super(MakeImportSubcommand, self).__init__(
            name='make-import',
            help='make-import TODO'
        )
    
    def func(self, *_):
        print(self.root_parser.format_help())


class RkbeetsCommand(Subcommand):
    item_types = {'rating': types.INTEGER}

    @property
    def album_types(self):
        return {'rating': types.INTEGER}

    command_name = 'rkbeets'
    parser: SubcommandsOptionParser = None
    config: Subview = None

    def __init__(self, cfg):
        self.config = cfg

        self.parser = SubcommandsOptionParser(
            usage='beet {name} TODO'.format(name=self.command_name)
        )

        self.parser.add_option(
            '-x', '--xml-file', dest='xml_filename',
            help=u'Rekordbox XML filename to use'
        )

        self.parser.add_subcommand(HelpSubcommand())
        self.parser.add_subcommand(ReportSubcommand())
        self.parser.add_subcommand(SyncSubcommand())
        self.parser.add_subcommand(MakeImportSubcommand())

        super(RkbeetsCommand, self).__init__(
            name=self.command_name,
            parser=self.parser,
            help='Rekordbox integration utilities',
        )

    def func(self, lib: Library, opts, args):
        xml_filename = self.resolve_xml_filename(opts.xml_filename)
        if xml_filename is None:
            print("Rekordbox XML file is required, specify in plugin configuration or with the -x command line argument.")
            self.parser.print_help()
            return

        subcommand, suboptions, subargs = self.parser.parse_subcommand(args)
        subcommand.func(xml_filename, lib, suboptions, subargs)

    def resolve_xml_filename(self, opt_xml_filename):
        xml_filename = opt_xml_filename
        if xml_filename is None:
            if self.config['xml_filename'].exists():
                xml_filename = self.config['xml_filename'].get()
        return xml_filename
