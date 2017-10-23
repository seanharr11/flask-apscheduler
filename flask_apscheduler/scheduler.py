# Copyright 2015 Vinicius Chiele. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""APScheduler implementation."""

import logging
import socket
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.manual import ManualTrigger
from . import views
from .utils import fix_job_def, pop_trigger, Singleton

LOGGER = logging.getLogger('flask_apscheduler')


class APScheduler(Singleton):
    """Provides a scheduler integrated to Flask."""

    def __init__(self, scheduler=None, app=None):
        self.__scheduler = scheduler or BackgroundScheduler()
        self.__allowed_hosts = ['*']
        self.__host_name = socket.gethostname().lower()
        self.__views_enabled = False

        self.app = None

        if app:
            self.init_app(app)

    @property
    def host_name(self):
        """Gets the host name."""
        return self.__host_name

    @property
    def allowed_hosts(self):
        """Gets the allowed hosts."""
        return self.__allowed_hosts

    @property
    def running(self):
        """Gets true whether the scheduler is running."""
        return self.scheduler.running

    @property
    def scheduler(self):
        """Gets the base scheduler."""
        return self.__scheduler

    def init_app(self, app):
        """Initializes the APScheduler with a Flask application instance."""
        ###############################
        ### Follow a singletone pattern
        ###############################

        self.app = app

        self.app.apscheduler = self

        self.__load_config()
        self.__load_jobs()

        if self.__views_enabled:
            self.__load_views()

    def start(self):
        """Starts the scheduler."""

        if self.host_name not in self.allowed_hosts and '*' not in self.allowed_hosts:
            LOGGER.debug('Host name %s is not allowed to start the APScheduler. Servers allowed: %s' %
                         (self.host_name, ','.join(self.allowed_hosts)))
            return

        self.__scheduler.start()

    def shutdown(self, wait=True):
        """
        Shuts down the scheduler. Does not interrupt any currently running jobs.

        :param bool wait: ``True`` to wait until all currently executing jobs have finished
        :raises SchedulerNotRunningError: if the scheduler has not been started yet
        """

        self.__scheduler.shutdown(wait)

    def add_job(self, id, func, **kwargs):
        """
        Adds the given job to the job list and wakes up the scheduler if it's already running.

        :param str id: explicit identifier for the job (for modifying it later)
        :param func: callable (or a textual reference to one) to run at the given time
        """

        job_def = dict(kwargs)
        job_def['id'] = id
        job_def['func'] = func
        job_def['name'] = job_def.get('name') or id

        fix_job_def(job_def)

        return self.__scheduler.add_job(**job_def)

    def delete_job(self, id, jobstore=None):
        """
        Removes a job, preventing it from being run any more.

        :param str id: the identifier of the job
        :param str jobstore: alias of the job store that contains the job
        """

        self.__scheduler.remove_job(id, jobstore)

    def delete_all_jobs(self, jobstore=None):
        """
        Removes all jobs from the specified job store, or all job stores if none is given.

        :param str|unicode jobstore: alias of the job store
        """

        self.__scheduler.remove_all_jobs(jobstore)

    def get_job(self, id, jobstore=None):
        """
        Returns the Job that matches the given ``id``.

        :param str id: the identifier of the job
        :param str jobstore: alias of the job store that most likely contains the job
        :return: the Job by the given ID, or ``None`` if it wasn't found
        :rtype: Job
        """

        return self.__scheduler.get_job(id, jobstore)

    def get_jobs(self, jobstore=None):
        """
        Returns a list of pending jobs (if the scheduler hasn't been started yet) and scheduled jobs, either from a
        specific job store or from all of them.

        :param str jobstore: alias of the job store
        :rtype: list[Job]
        """

        return self.__scheduler.get_jobs(jobstore)

    def modify_job(self, id, jobstore=None, **changes):
        """
        Modifies the properties of a single job. Modifications are passed to this method as extra keyword arguments.

        :param str id: the identifier of the job
        :param str jobstore: alias of the job store that contains the job
        """

        fix_job_def(changes)

        if 'trigger' in changes:
            trigger, trigger_args = pop_trigger(changes)
            self.__scheduler.reschedule_job(id, jobstore, trigger, **trigger_args)

        return self.__scheduler.modify_job(id, jobstore, **changes)

    def pause_job(self, id, jobstore=None):
        """
        Causes the given job not to be executed until it is explicitly resumed.

        :param str id: the identifier of the job
        :param str jobstore: alias of the job store that contains the job
        """

        self.__scheduler.pause_job(id, jobstore)

    def resume_job(self, id, jobstore=None):
        """
        Resumes the schedule of the given job, or removes the job if its schedule is finished.

        :param str id: the identifier of the job
        :param str jobstore: alias of the job store that contains the job
        """
        self.__scheduler.resume_job(id, jobstore)

    def get_job_submission(self, job_submission_id, jobstore=None):
        """
        Gets the specific job_submission by ID

        :param str|int id: the identifier of the job_submission
        :param str jobstore: alias of the jobstore that contains the job submission
        :rtype: dict
        """

        return self.__scheduler.get_job_submission(jobstore, job_submission_id)

    def get_job_submissions(self, jobstore=None):
        """
        Get all job_submissions in the jobstore

        :param str jobstore: alias of the jobstore that contains the job submission
        :rtype: list(dict)
        """

        return self.__scheduler.get_job_submissions(jobstore)

    def get_job_statuses(self, jobstore=None):
        """
        Get all the  of all jobs, where status one of the following:


           states = { 'ok', 'failing', 'orphaned', 'missed' }
           statuses = {'running', 'paused', 'scheduled', 'not scheduled' }

           1. 'ok':      Job is scheduled to be run, and has not recently failed
           2. 'running': Job is currently running, or waiting to be run in a queue on the executor
           3. 'paused':  Job is paused, and will not run until resumed
           4. 'failing': Job has failed, at least once, in the last 'n' minutes, where n is
           provided via the ``job_failing_window`` parameter.
           5. 'not scheduled': The job has trigger-type ``Date``, and has no ``next_run_time``.
           6. 'orphaned':      The job has recently been abandoned due to the scheduler crashing after submission
           7. 'missed':        The job has recently been missed due to the scheduler crashing before submission

        :rtype: list(dict)
           [{'job_id': 123, 'state': 'paused', 'status': 'ok'},
            {'job_id': 124, 'state': 'running', 'status': 'failing'}
            ...
            {'job_id': 1000, 'state': 'not scheduled', 'status': 'failing'}]
        """

        # TODO: This should happen at APScheduler level...
        jobs = self.__scheduler.get_jobs(jobstore)
        job_submissions = self.__scheduler.get_job_submissions(jobstore)

        job_subs_grouped_by_job_id = {}
        for js in job_submissions:
            job_id = js['apscheduler_job_id']
            if job_subs_grouped_by_job_id.get(job_id):
                job_subs_grouped_by_job_id[job_id].append(js)
            else:
                job_subs_grouped_by_job_id[job_id] = [js]
        # END TODO
        jobs_and_statuses = []
        job_failing_window = 720 # 12 hours

        for j in jobs:
            job_subs = job_subs_grouped_by_job_id.get(j.id)
            #########################
            ### Determine status
            #########################

            # OK Success states
            status = "ok"
            state = "scheduled"
            if j.next_run_time == None:
                if isinstance(j.trigger, DateTrigger) or isinstance(j.trigger, ManualTrigger):
                    state = "not scheduled"
                else:
                    state = "paused"
            if job_subs:
                job_subs.sort(key=lambda k: k['submitted_at'])
                failed_jobs_in_window = list(
                                         filter(lambda js: js['submitted_at'] >
                                            datetime.now() - timedelta(minutes = job_failing_window)
                                            and js['state'] == "failure", job_subs))
                most_recent_js = job_subs[-1]
                if most_recent_js['state'] == "submitted":
                    state = "running"
                # Warning states
                if most_recent_js['state'] == "missed":
                    status = "missed"
                if most_recent_js['state'] == "orphaned":
                    status = "orphaned"
                # Critical states
                if len(failed_jobs_in_window) > 0:
                    status = "failing"
            jobs_and_statuses.append({'job_id': j.id, 'state': state, 'status': status})
        return jobs_and_statuses

    def get_job_submissions_for_job(self, job_id, jobstore=None):
        """
        Get all job_submissions for a given job_id in the jobstore

        :param str jobstore: alias of the jobstore that contains the job submission
        :rtype: list(dict)

        """
        return sorted(self.__scheduler.get_job_submissions_for_job(jobstore, job_id),
                      key=lambda js: js['submitted_at'],
                      reverse=True)

    def run_job(self, id, job_kwargs=None, jobstore=None):
        job = self.__scheduler.get_job(id, jobstore)

        if not job:
            raise LookupError(id)

        if job_kwargs:
            # This does not persist to the jobstore !

            job._modify(**{'kwargs': job_kwargs})

        self.__scheduler._executors[job.executor]\
                        .submit_job(job, datetime.now())

        # job.func(*job.args, **job.kwargs)

    def __load_config(self):
        """Loads the configuration from the Flask configuration."""

        options = dict()

        job_stores = self.app.config.get('SCHEDULER_JOBSTORES')
        if job_stores:
            options['jobstores'] = job_stores

        executors = self.app.config.get('SCHEDULER_EXECUTORS')
        if executors:
            options['executors'] = executors

        job_defaults = self.app.config.get('SCHEDULER_JOB_DEFAULTS')
        if job_defaults:
            options['job_defaults'] = job_defaults

        timezone = self.app.config.get('SCHEDULER_TIMEZONE')
        if timezone:
            options['timezone'] = timezone

        self.__scheduler.configure(**options)

        self.__allowed_hosts = self.app.config.get('SCHEDULER_ALLOWED_HOSTS', self.__allowed_hosts)
        self.__views_enabled = self.app.config.get('SCHEDULER_VIEWS_ENABLED', self.__views_enabled)

    def __load_jobs(self):
        """Loads the job definitions from the Flask configuration."""

        jobs = self.app.config.get('SCHEDULER_JOBS')

        if not jobs:
            jobs = self.app.config.get('JOBS')

        if jobs:
            for job in jobs:
                self.add_job(**job)

    def __load_views(self):
        """Adds the routes for the scheduler UI."""

        self.app.add_url_rule('/scheduler', 'get_scheduler_info', views.get_scheduler_info)
        self.app.add_url_rule('/scheduler/jobs', 'add_job', views.add_job, methods=['POST'])
        self.app.add_url_rule('/scheduler/jobs', 'get_jobs', views.get_jobs)
        self.app.add_url_rule('/scheduler/jobs/<job_id>', 'get_job', views.get_job)
        self.app.add_url_rule('/scheduler/jobs/<job_id>', 'delete_job', views.delete_job, methods=['DELETE'])
        self.app.add_url_rule('/scheduler/jobs/<job_id>', 'update_job', views.update_job, methods=['PATCH'])
        self.app.add_url_rule('/scheduler/jobs/<job_id>/pause', 'pause_job', views.pause_job, methods=['POST'])
        self.app.add_url_rule('/scheduler/jobs/<job_id>/resume', 'resume_job', views.resume_job, methods=['POST'])
        self.app.add_url_rule('/scheduler/jobs/<job_id>/run', 'run_job', views.run_job, methods=['POST'])
        # Job Submission Routes
        self.app.add_url_rule('/scheduler/jobs/<job_id>/job_submissions', 'get_job_submissions_for_job', views.get_job_submissions_for_job, methods=['GET'])
        self.app.add_url_rule('/scheduler/job_submissions', 'get_job_submissions', views.get_job_submissions, methods=['GET'])
        self.app.add_url_rule('/scheduler/job_submissions/<job_submission_id>', 'get_job_submission', views.get_job_submission, methods=['GET'])
        self.app.add_url_rule('/scheduler/jobs/statuses', 'get_job_statuses', views.get_job_statuses, methods=['GET'])
