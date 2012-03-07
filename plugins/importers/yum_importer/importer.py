# -*- coding: utf-8 -*-
#
# Copyright © 2012 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.

"""
Importer plugin for Yum functionality
"""

import logging
import os
import time

import rpm
import errata
from pulp.server.content.plugins.importer import Importer
_LOG = logging.getLogger(__name__)
#TODO Fix up logging so we log to a separate file to aid debugging
#_LOG.addHandler(logging.FileHandler('/var/log/pulp/yum-importer.log'))

YUM_IMPORTER_TYPE_ID="yum_importer"

REQUIRED_CONFIG_KEYS = ['feed_url']
OPTIONAL_CONFIG_KEYS = ['sslcacert', 'sslclientcert', 'sslclientkey', 'sslverify', 
                        'proxy_url', 'proxy_port', 'proxy_pass', 'proxy_user',
                        'max_speed', 'verify_options', 'num_threads']

class YumImporter(Importer):
    PROGRESS_REPORT_FIELDS = ["items_total", "items_left", "size_total", "size_left", 
                        "item_name", "status", "item_type", "num_error", "num_success", 
                        "num_download", "details", "error_details", "step"]

    @classmethod
    def metadata(cls):
        return {
            'id'           : YUM_IMPORTER_TYPE_ID,
            'display_name' : 'Yum Importer',
            'types'        : [rpm.RPM_TYPE_ID, errata.ERRATA_TYPE_ID]
        }

    def validate_config(self, repo, config):
        _LOG.info("validate_config invoked, config values are: %s" % (config.repo_plugin_config))
        for key in REQUIRED_CONFIG_KEYS:
            if key not in config.repo_plugin_config:
                _LOG.error("Missing required configuration key: %s" % (key))
                return False
        for key in config.repo_plugin_config:
            if key not in REQUIRED_CONFIG_KEYS and key not in OPTIONAL_CONFIG_KEYS:
                _LOG.error("Configuration key '%s' is not supported" % (key))
                return False
        return True

    def importer_added(self, repo, config):
        _LOG.info("importer_added invoked")

    def importer_removed(self, repo, config):
        _LOG.info("importer_removed invoked")

    def import_units(self, repo, units, import_conduit, config):
        """
        Import content units into the given repository. This method will be
        called in a number of different situations:
         * A user is attempting to migrate a content unit from one repository
           into the repository that uses this importer
         * A user has uploaded a content unit to the Pulp server and is
           attempting to associate it to a repository that uses this importer
         * An existing repository is being cloned into a repository that
           uses this importer

        In all cases, the expected behavior is that the importer uses this call
        as an opportunity to perform any changes it needs to its working
        files for the repository to incorporate the new units.

        The units may or may not exist in Pulp prior to this call. The call to
        add a unit to Pulp is idempotent and should be made anyway to ensure
        the case where a new unit is being uploaded to Pulp is handled.

        @param repo: metadata describing the repository
        @type  repo: L{pulp.server.content.plugins.data.Repository}

        @param units: list of objects describing the units to import in
                      this call
        @type  units: list of L{pulp.server.content.plugins.data.Unit}

        @param import_conduit: provides access to relevant Pulp functionality
        @type  import_conduit: ?

        @param config: plugin configuration
        @type  config: L{pulp.server.content.plugins.config.PluginCallConfiguration}
        """
        _LOG.info("import_units invoked")

    def remove_units(self, repo, units, remove_conduit):
        _LOG.info("remove_units invoked for %s units" % (len(units)))

    # -- actions --------------------------------------------------------------

    def sync_repo(self, repo, sync_conduit, config):
        summary, details = self._sync_repo(repo, sync_conduit, config)
        return sync_conduit.build_report(summary, details)

    def _sync_repo(self, repo, sync_conduit, config):
        def progress_callback(report):
            """
            Translates grinders progress report to a dict Pulp can use
            @param report progress report
            @type report: grinder.GrinderCallback.ProgressReport
            """
            status = {}
            for f in self.PROGRESS_REPORT_FIELDS:
                status[f] = getattr(report, f)
            sync_conduit.set_progress(status)
        # sync rpms
        rpm_summary, rpm_details = rpm._sync_rpms(repo, sync_conduit, config, progress_callback)
        # sync errata
        errata_summary, errata_details = errata._sync_errata(repo, sync_conduit, config, progress_callback)
        summary = dict(rpm_summary.items() + errata_summary.items())
        details = dict(rpm_details.items() + errata_details.items())
        return summary, details
