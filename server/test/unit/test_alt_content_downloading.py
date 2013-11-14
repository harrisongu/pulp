# Copyright (c) 2013 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.

import os
import shutil

from uuid import uuid4
from tempfile import mkdtemp
from mock import patch

from nectar.downloaders.local import LocalFileDownloader

from base import PulpAsyncServerTests

from pulp.server.db.model.content import ContentCatalog
from pulp.server.managers.content.download import DownloadManager, Unit

PRIMARY = 'primary'
UNIT_WORLD = 'unit-world'

TYPE_ID = 'rpm'


ALT_1 = """
[%s]
enabled: 1
name: Unit World
priority: 1
cataloger: test
""" % UNIT_WORLD


class TestDownloading(PulpAsyncServerTests):

    def setUp(self):
        PulpAsyncServerTests.setUp(self)
        ContentCatalog.get_collection().remove()
        self.tmp_dir = mkdtemp()
        self.downloaded = os.path.join(self.tmp_dir, 'downloaded')
        os.makedirs(self.downloaded)
        self.add_sources()

    def tearDown(self):
        PulpAsyncServerTests.tearDown(self)
        ContentCatalog.get_collection().remove()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def add_sources(self):
        # unit-world
        path = os.path.join(self.tmp_dir, 'unit-world.conf')
        with open(path, 'w+') as fp:
            fp.write(ALT_1)

    def populate_content(self, source, n_start, n_units):
        _dir = os.path.join(self.tmp_dir, source)
        os.makedirs(_dir)
        for n in range(n_start, n_start + n_units):
            path = os.path.join(_dir, 'unit_%d' % n)
            with open(path, 'w+') as fp:
                fp.write(path)
        return _dir

    def populate_catalog(self, source_id, n_start, n_units):
        _dir = self.populate_content(source_id, n_start, n_units)
        collection = ContentCatalog.get_collection()
        units = []
        for n in range(n_start, n_start + n_units):
            unit_key = {
                'name': 'unit_%d' % n,
                'version': '1.0.%d' % n,
                'release': '1',
                'checksum': str(uuid4())
            }
            url = 'file://%s/unit_%d' % (_dir, n)
            unit = ContentCatalog(source_id, TYPE_ID, unit_key, url)
            units.append(unit)
        for unit in units:
            collection.insert(unit, safe=True)
        return units

    @patch('pulp.server.managers.content.download.Downloader', LocalFileDownloader)
    def test_download(self):
        units = []
        cataloged = self.populate_catalog(UNIT_WORLD, 0, 10)
        _dir = self.populate_content(PRIMARY, 0, 20)
        # unit-world
        for n in range(0, 10):
            unit = Unit(
                cataloged[n].type_id,
                cataloged[n].unit_key,
                'file://%s/unit_%d' % (_dir, n),
                os.path.join(self.downloaded, 'unit_%d' % n))
            units.append(unit)
        # primary
        for n in range(11, 20):
            unit_key = {
                'name': 'unit_%d' % n,
                'version': '1.0.%d' % n,
                'release': '1',
                'checksum': str(uuid4())
            }
            unit = Unit(
                TYPE_ID,
                unit_key,
                'file://%s/unit_%d' % (_dir, n),
                os.path.join(self.downloaded, 'unit_%d' % n))
            units.append(unit)
        mgr = DownloadManager(self.tmp_dir)
        mgr.download(None, units)
        # unit-world
        for i in range(0, 10):
            unit = units[i]
            self.assertTrue(unit.downloaded)
            self.assertEqual(len(unit.errors), 0)
            with open(unit.destination) as fp:
                s = fp.read()
                self.assertTrue(UNIT_WORLD in s)
        # primary
        for i in range(11, len(units)):
            unit = units[i]
            self.assertTrue(unit.downloaded)
            self.assertEqual(len(unit.errors), 0)
            with open(unit.destination) as fp:
                s = fp.read()
                self.assertTrue(PRIMARY in s)