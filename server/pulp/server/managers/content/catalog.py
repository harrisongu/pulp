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

from pulp.server.db.model.content import ContentCatalog


class ContentCatalogManager(object):

    def add_entry(self, source_id, type_id, unit_key, url):
        collection = ContentCatalog .collection()
        entry = ContentCatalog (source_id, type_id, unit_key, url)
        collection.insert(entry, safe=True)

    def delete_entry(self, source_id, type_id, unit_key):
        collection = ContentCatalog .collection()
        locator = ContentCatalog .get_locator(type_id, unit_key)
        query = dict(source_id=source_id, locator=locator)
        collection.delete(query, safe=True)

    def purge(self, source_id):
        collection = ContentCatalog .collection()
        query = dict(source_id=source_id)
        collection.delete(query, safe=True)

    def find(self, type_id, unit_key):
        collection = ContentCatalog.get_collection()
        locator = ContentCatalog .get_locator(type_id, unit_key)
        query = dict(locator=locator)
        cursor = collection.find(query)
        return list(cursor)