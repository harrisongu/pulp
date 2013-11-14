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

from ConfigParser import ConfigParser

from nectar.listener import DownloadEventListener
from nectar.config import DownloaderConfig
from nectar.request import DownloadRequest
from nectar.downloaders.threaded import HTTPThreadedDownloader as Downloader

from pulp.server.managers.content.catalog import UnitCatalogManager


# --- constants --------------------------------------------------------------


URL = 'url'
SOURCE_ID = 'source_id'
TYPE_ID = 'type_id'
UNIT_KEY = 'unit_key'
PRIORITY = 'priority'
ENABLED = 'enabled'
DESTINATION = 'destination'


# --- download manager -------------------------------------------------------


class DownloadManager(object):

    CONF_D = '/etc/pulp/plugins/cataloger/conf.d/'

    @staticmethod
    def load(conf_d=CONF_D):
        sources = {}
        for name in os.listdir(conf_d):
            path = os.path.join(conf_d, name)
            cfg = ConfigParser()
            cfg.read(path)
            for section in cfg.sections():
                descriptor = dict(cfg.items(section))
                source = ContentSource(section, descriptor)
                if not source.enabled():
                    continue
                sources[source.id] = source
        return sources

    @staticmethod
    def collated(units):
        collated = {}
        for unit in units:
            if unit.downloaded:
                continue
            source = unit.next_source()
            if source is None:
                continue
            request_list = collated.setdefault(source[0], [])
            request = DownloadRequest(source[1], unit.destination, data=unit)
            request_list.append(request)
        return collated

    def __init__(self, path=None):
        self.sources = DownloadManager.load(path)

    def download(self, downloader, units):
        primary = PrimarySource(downloader)
        for unit in units:
            unit.find_sources(primary, self.sources)
        while True:
            collated = self.collated(units)
            if not collated:
                break
            for source, request_list in collated.items():
                if not request_list:
                    continue
                downloader = source.downloader()
                downloader.event_listener = Listener()
                downloader.download(request_list)


class Unit(object):

    def __init__(self, type_id, unit_key, url, destination):
        self.type_id = type_id
        self.key = unit_key
        self.url = url
        self.destination = destination
        self.downloaded = False
        self.sources = []
        self.index = 0
        self.errors = []

    def find_sources(self, primary, alternates):
        resolved = [(primary, self.url)]
        manager = UnitCatalogManager()
        for entry in manager.find(self.type_id, self.key):
            source_id = entry[SOURCE_ID]
            source = alternates.get(source_id)
            if source is None:
                continue
            url = entry[URL]
            resolved.append((source, url))
        resolved.sort()
        self.sources = resolved

    def next_source(self):
        if self.has_source():
            source = self.sources[self.index]
            self.index += 1
            return source

    def has_source(self):
        return self.index < len(self.sources)


# --- implementation details -------------------------------------------------


class ContentSource(object):

    def __init__(self, source_id, descriptor):
        self.id = source_id
        self.descriptor = descriptor

    def enabled(self):
        enabled = self.descriptor.get(ENABLED)
        return enabled.lower() in ('1', 'true', 'yes')

    def priority(self):
        return int(self.descriptor.get(PRIORITY, 0))

    def downloader(self):
        conf = DownloaderConfig()
        downloader = Downloader(conf)
        return downloader

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return self.id != other.id

    def __hash__(self):
        return hash(self.id)

    def __gt__(self, other):
        return self.priority() > other.priority()

    def __lt__(self, other):
        return self.priority() < other.priority()


class PrimarySource(ContentSource):

    def __init__(self, downloader):
        ContentSource.__init__(self, '__primary__', {})
        self.__downloader = downloader

    def priority(self):
        return 0xAAAA


class Listener(DownloadEventListener):

    def download_succeeded(self, report):
        unit = report.data
        unit.downloaded = True

    def download_failed(self, report):
        unit = report.data
        unit.errors.append(report.error_msg)