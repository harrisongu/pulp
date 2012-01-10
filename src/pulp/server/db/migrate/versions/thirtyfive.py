
# -*- coding: utf-8 -*-
#
# Copyright © 2011 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.

import logging
from pulp.server.db.model import Package, Errata, Repo

_LOG = logging.getLogger('pulp')

version = 35

def _migrate_packages():
    pkg_collection = Package.get_collection()
    repo_collection = Repo.get_collection()
    all_packages = list(pkg_collection.find())
    _LOG.info('migrating %s packages' % len(all_packages))
    for pkg in all_packages:
        try:
            modified = False
            found = repo_collection.find({"packages":pkg['id']}, fields=["id"])
            repos = [r["id"] for r in found]
            if not pkg.has_key('repoids') or not pkg['repoids']:
                pkg['repoids'] = repos
                modified =True
            if modified:
                pkg_collection.save(pkg, safe=True)
        except Exception, e:
            _LOG.critical(e)
            return False
    return True

def _migrate_errata():
    collection = Errata.get_collection()
    all_errata = list(collection.find())
    _LOG.info('migrating %s errata' % len(all_errata))
    for e in all_errata:
        try:
            modified = False
            repos = find_errata_repos(e['id'])
            if not e.has_key('repoids') or not e['repoids']:
                e['repoids'] = repos
                modified =True
            if modified:
                collection.save(e, safe=True)
        except Exception, e:
            _LOG.critical(e)
            return False
    return True

def find_errata_repos(errata_id):
    """
    Return repos that contain passed in errata_id
    """
    repos = []
    collection = Repo.get_collection()
    all_repos = list(collection.find())
    for r in all_repos:
        for e_type in r["errata"]:
            if errata_id in r["errata"][e_type]:
                repos.append(r["id"])
                break
    return repos

def migrate():
    _LOG.info('migrating packages to include repoids field')
    _migrate_packages()
    _LOG.info('migrating errata to include repoids field')
    _migrate_errata()
