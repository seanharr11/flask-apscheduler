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

from apscheduler.jobstores.base import ConflictingIdError, JobLookupError
from collections import OrderedDict
from flask import current_app, request, Response
from .json import jsonify


def get_scheduler_info():
    """Gets the scheduler info."""

    scheduler = current_app.apscheduler

    d = OrderedDict([
        ('current_host', scheduler.host_name),
        ('allowed_hosts', scheduler.allowed_hosts),
        ('running', scheduler.running)
    ])

    return jsonify(d)


def get_job_submissions():
    job_submissions = current_app.apscheduler.get_job_submissions()
    return jsonify(job_submissions)

def get_job_submission(job_submission_id):
    job_submission = current_app.apscheduler.get_job_submission(job_submission_id)
    if not job_submission:
        return jsonify(dict(error_message='Job Submission %s not found' % job_submission_id), status=404)
    return jsonify(job_submission)   

def get_running_jobs():
    return jsonify(current_app.apscheduler.get_running_jobs())

def get_job_statuses():
    return jsonify(current_app.apscheduler.get_job_statuses())

def get_job_submissions_for_job(job_id):
    job_submissions = current_app.apscheduler.get_job_submissions_for_job(job_id)
    return jsonify(job_submissions)

def add_job():
    """Adds a new job."""

    data = request.get_json(force=True)
 
    try:
        job = current_app.apscheduler.add_job(**data)
        return jsonify(job)
    except ConflictingIdError:
        return jsonify(dict(error_message='Job %s already exists.' % data.get('id')), status=409)
    except Exception as e:
        return jsonify(dict(error_message=str(e)), status=500)


def delete_job(job_id):
    """Deletes a job."""

    try:
        current_app.apscheduler.delete_job(job_id)
        return Response(status=204)
    except JobLookupError:
        return jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return jsonify(dict(error_message=str(e)), status=500)


def get_job(job_id):
    """Gets a job."""

    job = current_app.apscheduler.get_job(job_id)

    if not job:
        return jsonify(dict(error_message='Job %s not found' % job_id), status=404)

    return jsonify(job)


def get_jobs():
    """Gets all scheduled jobs."""

    jobs = current_app.apscheduler.get_jobs()

    job_states = []

    for job in jobs:
        job_states.append(job)

    return jsonify(job_states)


def update_job(job_id):
    """Updates a job."""

    data = request.get_json(force=True)

    try:
        current_app.apscheduler.modify_job(job_id, **data)
        job = current_app.apscheduler.get_job(job_id)
        return jsonify(job)
    except JobLookupError:
        return jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return jsonify(dict(error_message=str(e)), status=500)


def pause_job(job_id):
    """Pauses a job."""

    try:
        current_app.apscheduler.pause_job(job_id)
        job = current_app.apscheduler.get_job(job_id)
        return jsonify(job)
    except JobLookupError:
        return jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return jsonify(dict(error_message=str(e)), status=500)


def resume_job(job_id):
    """Resumes a job."""

    try:
        current_app.apscheduler.resume_job(job_id)
        job = current_app.apscheduler.get_job(job_id)
        return jsonify(job)
    except JobLookupError:
        return jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return jsonify(dict(error_message=str(e)), status=500)


def run_job(job_id):
    """Executes a job."""

    kwargs = request.get_json(force=True)
    
    try:
        current_app.apscheduler.run_job(job_id, job_kwargs=kwargs)
        job = current_app.apscheduler.get_job(job_id)
        return jsonify(job)
    except LookupError:
        return jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return jsonify(dict(error_message=str(e)), status=500)
