#!/usr/bin/env python

import gevent
from gevent import monkey
# gevent says monkey-patch as early as possible
monkey.patch_all()

from flask import Flask
from flask import json
from flask import request
from flask import Response
import gevent.pywsgi
import gevent.socket
import logging
import os
import os.path
import shutil
import subprocess
import sys
import yaml

import mail_utils

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    level=logging.INFO)

app = Flask(__name__)
SMTP_HOST = 'localhost'
CONFIG_FILE = 'server_config.yaml'
JOBS_DIR_PATH = os.path.join(os.getcwd(), 'jobs')


class Request(object):
    """A request is json from a client asking to run a job.
    It runs in its own directory.
    """
    SEMAPHORE = gevent.lock.Semaphore()
    # Where a request gets its own directory
    DIR_PATH = os.path.join(os.getcwd(), 'request_dirs')
    # Global var to get a new request number
    __request_num = 0
    # Map of all requests
    # __request_map = {}

    @staticmethod
    def request_path(request_num):
        return os.path.join(Request.DIR_PATH, str(request_num))

    def __init__(self, request_json):
        self.job_name = request_json.get('job_name', '')
        self.job_path = make_job_path(self.job_name)
        self.email_results_to = request_json.get('email_results_to')
        self.files = request_json.get('files', {})
        self.leave_output = request_json.get('leave_output', False)
        # self.request_json is for logging
        self.request_json = request_json

    def _create_request_dir(self):
        """Create a new request dir.

        Assign self.request_num, self.request_dir.
        """
        Request.SEMAPHORE.acquire()
        Request.__request_num += 1
        while os.path.exists(Request.request_path(Request.__request_num)):
            Request.__request_num += 1
        self.request_num = Request.__request_num
        self.request_dir = Request.request_path(self.request_num)
        os.mkdir(self.request_dir)
        # Request.__request_map[self.request_num] = self
        Request.SEMAPHORE.release()

    def sanity_check(self):
        """Return a Response if request params are wrong, else None."""
        if not self.job_name:
            return Response(json.dumps({'error': 'No job_name given'}),
                            status=400)
        if not os.path.exists(self.job_path):
            return Response(
                json.dumps({"error": "No job %s" % request.job_name}),
                status=400)
        if not (os.path.isfile(self.job_path) and
                os.access(self.job_path, os.X_OK)):
            return Response(
                json.dumps({"error": "Job %s is not an executable file"
                            % self.job_name}),
                status=400)
        return None

    def _email_results(self):
        """Email results if self.email_results_to is set.

        Files listed in return_files.txt are attached.
        """
        if not self.email_results_to:
            return

        # pack up the output and email
        # if there are any return files, attach them
        return_files_path = os.path.join(self.request_dir, 'return_files.txt')
        attachments = []
        if os.path.isfile(return_files_path):
            for filename in open(return_files_path).readlines():
                filepath = os.path.join(self.request_dir, filename.strip())
                logging.info("Try attaching %s to email" % filepath)
                if os.path.isfile(filepath):
                    attachments.append(
                        mail_utils.attachment_from_file(filepath))
                else:
                    logging.warning("%s is not a file" % filepath)
        mail_utils.send_simple_mail(
            SMTP_HOST, "job_server", self.email_results_to,
            "Results of %s" % self.job_name,
            "stdout:\n%s\n\nstderr:\n%s\n\n" % (
                self.stdoutdata, self.stderrdata),
            attachments=attachments)

    def _put_files(self):
        """Put contents of files into self.request_dir.

        files is a map where the keys are treated as filenames
        and the values as file contents.
        """
        for name, contents in self.files.iteritems():
            path = os.path.join(self.request_dir, name)
            if os.path.exists(path):
                logging.warning("Overwriting %s" % path)
            else:
                logging.info("Writing %s" % path)
            with open(path, "wb") as f:
                f.write(contents)

    def run_job(self):
        """Run the job in its own request directory"""
        self._create_request_dir()
        logging.info("request %d: json %s" % (
            self.request_num, json.dumps(self.request_json, indent=4)))
        # Put files from request into request_dir
        self._put_files()
        self.stdoutdata, self.stderrdata = run_command(
            self.request_dir, [self.job_path, str(self.request_num)])

        self._email_results()
        # remove request directory
        if not self.leave_output:
            shutil.rmtree(self.request_dir)


def server_config():
    if os.path.exists(CONFIG_FILE):
        doc = yaml.load(open(CONFIG_FILE))
        logging.info('config:\n%s' % open(CONFIG_FILE).read())
        mail_utils.mail_filter = doc.get('mail_filter', '')
    else:
        logging.warning("No %s" % CONFIG_FILE)


def make_job_path(job_name):
    return os.path.join(JOBS_DIR_PATH, job_name)


def run_command(request_dir, args):
    """run a command and capture stdout and stderr

    Return stdoutdata, stderrdata from subprocess.communicate().

    gevent-compatible.
    Example args: ['/bin/ls', '-lR']
    """
    logging.info("Run: %s in %s" % (args, request_dir))
    process = subprocess.Popen(
        # args passed to /bin/sh -c args[0] args[1] ...
        ' '.join(["cd %s;" % request_dir] + args),
        shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # communicate has been monkey-patched so gevent does not block
    stdoutdata, stderrdata = process.communicate()
    logging.info("Ended: %s in %s" % (args, request_dir))
    return stdoutdata, stderrdata


def jobs_start(request_json):
    request = Request(request_json)
    resp = request.sanity_check()
    if resp:
        return resp

    gevent.spawn(request.run_job())

    # Respond
    resp = {'request_number': request.request_num}
    return Response(json.dumps(resp), status=200, mimetype='application/json')


@app.route("/")
def hello():
    return __name__


@app.route("/jobs", methods=['GET', 'POST'])
def jobs():
    if request.method == 'GET':
        return "TODO: show jobs"
    elif request.method == 'POST':
        ct = request.headers['Content-Type']
        if ct == 'application/json':
            return jobs_start(request.json)
        else:
            # unsupported media type
            return Response(status=415)
    else:
        # method not allowed
        return Response(405)


def main():
    gevent.socket.setdefaulttimeout(30000)
    port = 6000
    if len(sys.argv) == 2:
        port = int(sys.argv[-1])
    elif len(sys.argv) > 2:
        print >>sys.stderr, "Usage: %s [port]" % __file__
        sys.exit(1)
    logging.info("Running on port: %s" % port)
    server_config()
    gevent.pywsgi.WSGIServer(("0.0.0.0", port), app).serve_forever()


if __name__ == "__main__":
    main()
