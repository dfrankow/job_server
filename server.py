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

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
__request_num = 0
SMTP_HOST = 'localhost'
CONFIG_FILE = 'server_config.yaml'
REQUEST_DIR_PATH = os.path.join(os.getcwd(), 'request_dirs')
REQUEST_DIR_SEMAPHORE = gevent.lock.Semaphore()
JOBS_DIR_PATH = os.path.join(os.getcwd(), 'jobs')


def config():
    if os.path.exists(CONFIG_FILE):
        doc = yaml.load(open(CONFIG_FILE))
        logging.info('config:\n%s' % open(CONFIG_FILE).read())
        mail_utils.mail_filter = doc.get('mail_filter', '')
    else:
        logging.warning("No %s" % CONFIG_FILE)


def request_path(request_num):
    return os.path.join(REQUEST_DIR_PATH, str(request_num))


def job_path(job_name):
    return os.path.join(JOBS_DIR_PATH, job_name)


def create_request_dir():
    """Create a new request dir.

    Return request_num, request_dir_path"""
    global __request_num

    REQUEST_DIR_SEMAPHORE.acquire()
    __request_num += 1
    while os.path.exists(request_path(__request_num)):
        __request_num += 1
    my_request_num = __request_num
    REQUEST_DIR_SEMAPHORE.release()
    the_path = request_path(my_request_num)
    os.mkdir(the_path)
    return my_request_num, the_path


def do_job(request_num, request_dir, job_name):
    return run_command(
        request_dir,
        [os.path.join(JOBS_DIR_PATH, job_name), str(request_num)])


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


def email_results(email_results_to, request_dir, job_name, stdoutdata, stderrdata):
    if email_results_to:
        # pack up the output and email
        # if there are any return files, attach them
        return_files_path = os.path.join(request_dir, 'return_files.txt')
        attachments = []
        if os.path.isfile(return_files_path):
            for filename in open(return_files_path).readlines():
                filepath = os.path.join(request_dir, filename.strip())
                logging.info("Try attaching %s to email" % filepath)
                if os.path.isfile(filepath):
                    attachments.append(mail_utils.attachment_from_file(filepath))
                else:
                    logging.warning("%s is not a file" % filepath)
        mail_utils.send_simple_mail(
            SMTP_HOST, "job_server", email_results_to, "Results of %s" % job_name,
            "stdout:\n%s\n\nstderr:\n%s\n\n" % (stdoutdata, stderrdata),
            attachments = attachments)


def put_files(request_dir, files):
    """Put contents of files into request_dir.

    files is a map where the keys are treated as filenames
    and the values as file contents.
    """
    for name, contents in files.iteritems():
        path = os.path.join(request_dir, name)
        if os.path.exists(path):
            logging.warning("Overwriting %s" % path)
        else:
            logging.info("Writing %s" % path)
        with open(path, "wb") as f:
            f.write(contents)


def jobs_start(request_json):
    # Check preconditions
    job_name = request_json.get('job_name', '')
    if not job_name:
        return Response(json.dumps({'error': 'No job_name given'}), status=400)
    the_job_path = job_path(job_name)
    if not os.path.exists(the_job_path):
        return Response(
            json.dumps({"error": "No job %s" % job_name}), status=400)
    if not (os.path.isfile(the_job_path) and os.access(the_job_path, os.X_OK)):
        return Response(
            json.dumps({"error": "Job %s is not an executable file"
                        % job_name}),
            status=400)

    # Do the job in its own request directory
    request_num, request_dir = create_request_dir()
    logging.info("request %d: json %s" % (request_num, request_json))
    # Put files from request into request_dir
    put_files(request_dir, request_json.get('files', {}))
    stdoutdata, stderrdata = do_job(request_num, request_dir, job_name)
    email_results(request_json.get('email_results_to'), request_dir, job_name,
                  stdoutdata, stderrdata)
    # remove request directory
    if not request_json.get('leave_output'):
        shutil.rmtree(request_dir)

    # Respond
    resp = {'request_number': request_num,
            'stdout': stdoutdata,
            'stderr': stderrdata}
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
    config()
    gevent.pywsgi.WSGIServer(("0.0.0.0", port), app).serve_forever()


if __name__ == "__main__":
    main()
