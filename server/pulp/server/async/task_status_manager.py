# -*- coding: utf-8 -*-
#
# Copyright © 2013 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.

from pulp.server.db.model.dispatch import TaskStatus
from pulp.server.exceptions import DuplicateResource, InvalidValue, MissingResource

class TaskStatusManager(object):
    """
    Performs task status related functions including both CRUD operations and queries on task statuses. 
    Task statuses returned by query calls are TaskStatus SON objects from the database.
    """

    @staticmethod
    def create_task_status(task_id, tags=[], state=None):
        """
        Creates a new task status for given task_id. 

        :param task_id: identity of the task this status corresponds to
        :type  task_id: basestring
        :param tags: custom tags on the task
        :type  tags: list
        :param state: state of callable in its lifecycle
        :type  state: basestring

        :raise DuplicateResource: if there is already a task status entry with the requested task id
        :raise InvalidValue: if any of the fields are unacceptable
        """

        existing_task_status = TaskStatus.get_collection().find_one({'task_id' : task_id})
        if existing_task_status is not None:
            raise DuplicateResource(task_id)

        invalid_values = []
        if task_id is None:
            invalid_values.append('task_id')
        if tags is not None and not isinstance(tags, list):
            invalid_values.append('tags')
        if state is not None and not isinstance(state, basestring):
            invalid_values.append('state')
        if invalid_values:
            raise InvalidValue(invalid_values)

        task_status = TaskStatus(task_id, tags=tags, state=state)
        TaskStatus.get_collection().save(task_status, safe=True)
        created = TaskStatus.get_collection().find_one({'task_id' : task_id})
        return created

    @staticmethod
    def get_or_create_task_status(task_id):
        """
        Returns an existing task status corresponding to the given task id.
        If it doesn't exist, creates a new TaskStatus and returns it.

        :param task_id: identity of the task
        :type  task_id: basetring
        :return: serialized data describing the task status
        :rtype:  dict
        """
        task_status = TaskStatus.get_collection().find_one({'task_id' : task_id})
        if task_status is None:
            task_status = TaskStatus(task_id)
            TaskStatus.get_collection().save(task_status, safe=True)
            task_status = TaskStatus.get_collection().find_one({'task_id' : task_id})
        
        return task_status

    @staticmethod
    def update_task_status(task_id, delta):
        """
        Updates status of the task with given task id. Only the following
        fields may be updated through this call:
        * state
        * result
        * traceback
        * start_time
        * finish_time
        Other fields found in delta will be ignored.

        :param task_id: identity of the task
        :type  task_id: basetring
        :param delta: list of attributes and their new values to change
        :type  delta: dict
        :raise MissingResource: if there is no task status corresponding to the given task_id
        """

        task_status = TaskStatus.get_collection().find_one({'task_id' : task_id})
        if task_status is None:
            raise MissingResource(task_id)

        updatable_attributes = ['state', 'result', 'traceback', 'start_time', 'finish_time']
        for key, value in delta.items():
            if key in updatable_attributes:
                task_status[key] = value
                
        TaskStatus.get_collection().save(task_status, safe=True)
        return task_status

    @staticmethod
    def find_all():
        """
        Returns serialized versions of all task statuses in the database.

        :return: list of serialized task statuses
        :rtype:  list of dict
        """
        all_task_statuses = list(TaskStatus.get_collection().find())
        return all_task_statuses

    @staticmethod
    def find_by_task_id(task_id):
        """
        Returns a serialized version the status of given task, if it exists.
        If a task status cannot be found with the given task_id, None is returned.

        :return: serialized data describing the task status
        :rtype:  dict or None
        """
        task_status = TaskStatus.get_collection().find_one({'task_id' : task_id})
        return task_status

    @staticmethod
    def find_by_criteria(criteria):
        """
        Return a list of task statuses that match the provided criteria.

        :param criteria:    A Criteria object representing a search you want
                            to perform
        :type  criteria:    pulp.server.db.model.criteria.Criteria
        :return:    list of TaskStatus instances
        :rtype:     list
        """
        return TaskStatus.get_collection().query(criteria)
