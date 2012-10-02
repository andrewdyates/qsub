#!/usr/name/python
"""Simple qsub dispatcher.

TODO: intelligently handle memory requests and request for multiple nodes and processors.

OSC qsub commands and environment
--------------------
https://osc.edu/book/export/html/2830
https://www.osc.edu/supercomputing/batch-processing-at-osc/pbs-directives-summary
https://www.osc.edu/documentation/batch-processing-at-osc/job-scripts
https://www.osc.edu/supercomputing/batch-processing-at-osc/job-submission
==============================

EXAMPLE USE:

# SIMPLE SCRIPT
# --------------------
from qsub import *
script_txt= "echo 'hello, world'"
qsub_txt = fill_template(jobname="hellworld", script=script_txt)
print "Submitting..."
print qsub_txt
submit(qsub_txt)

# USE OBJECT.
# --------------------
Q = Qsub(n_nodes=1)
Q.add("mycmd a=b")
Q.add_parallel(["python batch.py 1", "python batch.py 2"])
Q.add("mycmd a=c")
print Q.submit()
"""
import sys
import subprocess
import random
import datetime
import os

TEMPLATE = \
"""#PBS -N %(jobname)s
#PBS -l nodes=%(n_nodes)d:ppn=%(n_ppn)d
#PBS -j oe
#PBS -S /bin/bash
#PBS -l walltime=%(walltime)s
%(options)s
#tdate=$(date +%%T)

set -x
cd %(work_dir)s
source .bash_profile
%(script)s
"""
# Default work directory is user's home directory
WORK_DIR = os.environ["HOME"]
MAX_PPN_OAKLEY = 12
MAX_PPN_GLENN = 8

def get_mail_option(begin=True, end=True, abort=True):
  if not any((begin, end, abort)):
    return ""
  else:
    mail_option = "#PBS -m "
    if begin: mail_option += "b"
    if end: mail_option += "e"
    if abort: mail_option += "a"
    return mail_option

def get_set_env_op_threads(n):
  return "export OMP_NUM_THREADS=%d" % (n)


class Qsub(object):
  """Simple wrapper for qsub job building functionality."""
  def __init__(self, jobname=None, n_nodes=1, n_ppn=1, hours=2, minutes=0, seconds=0, options=None, work_dir=WORK_DIR, auto_time=True, email=False):
    self.jobname = jobname
    self.n_nodes = n_nodes
    self.n_ppn = n_ppn
    self.walltime = timestr(hours, minutes, seconds)
    if self.options is None:
      self.options = []
    else:
      self.options = options
    self.work_dir = work_dir
    self.auto_time = auto_time
    self.cmds = []
    if email:
      self.options.append(get_mail_option())


  def t(self, line):
    return precmd(cmd="time", line=line, cond=self.auto_time)

  def add_parallel(self, jobs):
    assert type(jobs) != str
    cmd = make_parallel(self.work_dir, self.jobname, jobs, self.auto_time)
    self.cmds.append(cmd)
    
  def add(self, job, simple=False, pernode=None):
    if not simple:
      job = self.t(job)
    if pernode is not None:
      assert pernode <= self.n_ppn and pernode >= 1
      precmd(cmd="time", line=job, cond=self.auto_time)
      job = precmd("mpiexec -npernode %d " % pernode, job)
    self.cmds.append(job)

  def echo(self, msg):
    self.cmds.append("echo %s" % msg)

  def qsub_script(self):
    """Return current qsub script that will be submitted."""
    script = "\n".join(self.cmds)
    options = "\n".join(self.options)
    return fill_template(jobname=self.jobname, n_nodes=self.n_nodes, n_ppn=self.n_ppn, walltime=self.walltime, options=options, script=script, work_dir=self.work_dir)

  def submit(self):
    """Submit qsub script, return job ID."""
    qsub_script = self.qsub_script()
    return submit(qsub_script)
    
    

def timestr(hours=0, minutes=0, seconds=0):
  return "%d:%.2d:%.2d" % (hours, minutes, seconds)

def make_parallel(work_dir, job_name, jobs, auto_time=True):
  fname = make_script_name(work_dir, job_name)
  fp = open(fname, "w")
  for job in jobs:
    batch_line = precmd("time", job, auto_time)
    fp.write(batch_line); fp.write('\n')
  fp.close()
  cmd_line = "mpiexec parallel-command-processor %s" % fname
  cmd_line = precmd("time", cmd_line, auto_time)
  return cmd_line

def fill_template(jobname="untitled", n_nodes=1, n_ppn=1, walltime='2:00:00', options="", script=None, work_dir=WORK_DIR, *vargs, **kwds):
  """Fill qsub submission script. Absorb any unrecognized keywords."""
  assert True or vargs is None or kwds is None # thwart pychecker warnings
  assert all((n_nodes, n_ppn, walltime, script, work_dir))
  assert type(options) == str
  n_ppn = int(n_ppn)
  n_nodes = int(n_nodes)
  return TEMPLATE % locals()

def submit(script_txt):
  # Get process ID
  p = subprocess.Popen("qsub", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, stderr = p.communicate(input=script_txt)
  p.stdin.close()
  return stdout.split('.')[0]


def tstamp():
  return datetime.datetime.isoformat(datetime.datetime.now())

def make_script_name(work_dir, job_name):
  random.seed()
  tmp_script_name = "tmp_parallel_script_%s_%s_%d.sh" % \
      (job_name, tstamp(), random.randint(0,10000000))
  dispatch_script_fname = os.path.join(work_dir, tmp_script_name)
  return dispatch_script_fname

def precmd(cmd, line, cond=True):
  if cond and line.partition(' ')[0] != cmd:
    return " ".join(([cmd] + line.split(' ')))
  else:
    return line

