# -*- coding: utf-8 -*-
#
# Copyright © 2011-2013 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.

from datetime import datetime
import pickle
from celery import current_app
from celery.beat import ScheduleEntry

from pulp.common import dateutils
from pulp.common.tags import resource_tag
from pulp.server.db.model.base import Model
from pulp.server.dispatch import constants as dispatch_constants
from pulp.server.webservices.serialization.db import scrub_mongo_fields


class CallResource(Model):
    """
    Information for an individual resource used by a call request.
    """

    collection_name = 'call_resources'
    search_indices = ('call_request_id', 'resource_type', 'resource_id')

    def __init__(self, call_request_id, resource_type, resource_id, operation):
        super(CallResource, self).__init__()
        self.call_request_id = call_request_id
        self.resource_type = resource_type
        self.resource_id  = resource_id
        self.operation = operation


class QueuedCall(Model):
    """
    Serialized queued call request
    """

    collection_name = 'queued_calls'
    unique_indices = ()

    def __init__(self, call_request):
        super(QueuedCall, self).__init__()
        self.serialized_call_request = call_request.serialize()
        self.timestamp = datetime.now()


class QueuedCallGroup(Model):
    """
    """

    collection_name = 'queued_call_groups'
    unique_indices = ('group_id',)

    def __init__(self, call_request_group_id, call_request_ids):
        super(QueuedCallGroup, self).__init__()

        self.call_request_group_id = call_request_group_id
        self.call_request_ids = call_request_ids

        self.total_calls = len(call_request_ids)
        self.completed_calls = 0


class OldScheduledCall(Model):
    """
    Serialized scheduled call request
    """

    collection_name = 'scheduled_calls'
    unique_indices = ()
    search_indices = ('serialized_call_request.tags', 'last_run', 'next_run')

    def __init__(self, call_request, schedule, failure_threshold=None, last_run=None, enabled=True):
        super(ScheduledCall, self).__init__()

        # add custom scheduled call tag to call request
        schedule_tag = resource_tag(dispatch_constants.RESOURCE_SCHEDULE_TYPE, str(self._id))
        call_request.tags.append(schedule_tag)

        self.serialized_call_request = call_request.serialize()

        self.schedule = schedule
        self.enabled = enabled

        self.failure_threshold = failure_threshold
        self.consecutive_failures = 0

        # scheduling fields
        self.first_run = None # will be calculated and set by the scheduler
        self.last_run = last_run and dateutils.to_naive_utc_datetime(last_run)
        self.next_run = None # will be calculated and set by the scheduler
        self.remaining_runs = dateutils.parse_iso8601_interval(schedule)[2]

        # run-time call group metadata for tracking success or failure
        self.call_count = 0
        self.call_exit_states = []


class ScheduledCall(Model):
    """
    Serialized scheduled call request
    """

    collection_name = 'scheduled_calls'
    unique_indices = ()
    search_indices = ('task.tags', 'last_run')

    def __init__(self, iso_schedule, task, total_run_count, next_run,
                 schedule, args, kwargs, principal, consecutive_failures=0, enabled=True, failure_threshold=None,
                 last_run_at=None, first_run=None, remaining_runs=None, id=None):
        """
        :type  schedule_entry:  celery.beat.ScheduleEntry

        """
        super(ScheduledCall, self).__init__()
        # add custom scheduled call tag to call request
        if isinstance(task, basestring):
            task = pickle.loads(task)

        self.id = id
        self.name = task.name
        self.next_run = next_run
        self.task = pickle.dumps(task)
        self.last_run_at = last_run_at
        self.total_run_count = total_run_count
        self.iso_schedule = iso_schedule
        self.app = current_app

        for key in ('schedule', 'args', 'kwargs', 'principal'):
            value = locals()[key]
            if isinstance(value, basestring):
                setattr(self, key, value)
            else:
                setattr(self, key, pickle.dumps(value))

        self.enabled = enabled

        self.failure_threshold = failure_threshold
        self.consecutive_failures = consecutive_failures

        if first_run is None:
            self.first_run = dateutils.format_iso8601_datetime(
                dateutils.parse_iso8601_interval(iso_schedule)[1])
        else:
            self.first_run = first_run
        if remaining_runs is None:
            self.remaining_runs = dateutils.parse_iso8601_interval(iso_schedule)[2]
        else:
            self.remaining_runs = remaining_runs

    @classmethod
    def from_db(cls, call):
        """
        :rtype:     pulp.server.db.model.dispatch.ScheduledCall
        """
        call = scrub_mongo_fields(call)
        call.pop('_id', None)
        call.pop('_ns', None)
        return cls(**call)

    def as_schedule_entry(self):
        last_run = dateutils.parse_iso8601_datetime(self.last_run_at)
        return ScheduleEntry(self.name, self.task, last_run, self.total_run_count,
                             pickle.loads(self.schedule), pickle.loads(self.args),
                             pickle.loads(self.kwargs), self.options,
                             self.relative, pickle.loads(self.app))

    @property
    def schedule_tag(self):
        """
        :return:    the resource tag appropriate for this schedule entry
        :rtype:     basestring
        """
        return resource_tag(dispatch_constants.RESOURCE_SCHEDULE_TYPE, str(self._id))

    @staticmethod
    def explode_schedule_entry(entry):
        """
        :param entry:
        :type  entry:   celery.beat.ScheduleEntry

        :return:    dict of data from a ScheduleEntry as it should be represented
                    to pass into the constructor of this class
        :rtype:     dict
        """
        schedule_keys = ('name', 'task', 'last_run_at', 'total_run_count',
                         'schedule', 'args', 'kwargs', 'app')
        return dict((k, getattr(entry, k)) for k in schedule_keys)


class ArchivedCall(Model):
    """
    Call history
    """

    collection_name = 'archived_calls'
    unique_indices = ()
    search_indices = ('serialized_call_report.call_request_id', 'serialized_call_report.call_request_group_id')

    def __init__(self, call_request, call_report):
        super(ArchivedCall, self).__init__()
        self.timestamp = dateutils.now_utc_timestamp()
        self.call_request_string = str(call_request)
        self.serialized_call_report = call_report.serialize()
