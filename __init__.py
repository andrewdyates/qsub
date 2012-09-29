#!/usr/name/python
"""Simple qsub dispatcher.

TODO: intelligently handle memory requests and request for multiple nodes and processors.

OSC qsub commands and environment
--------------------
https://osc.edu/book/export/html/2830
https://www.osc.edu/supercomputing/batch-processing-at-osc/pbs-directives-summary
https://www.osc.edu/documentation/batch-processing-at-osc/job-scripts
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
cd %(work_dir)
source .bash_profile
%(script)s
"""
# Default work directory is user's home directory
WORK_DIR = os.environ["HOME"]

def split_job(n):
  return 

class Qsub(object):
  """Simple wrapper for qsub job building functionality."""
  def __init__(self, jobname=None, n_nodes=1, n_ppn=1, hours=2, minutes=0, seconds=0, options="", work_dir=WORK_DIR, auto_time=True):
    self.jobname = jobname
    self.n_nodes = n_nodes
    self.n_ppn = n_ppn
    self.walltime = timestr(hours, minutes, seconds)
    self.options = options
    self.cmds = []
    self.auto_time = auto_time
    self.work_dir = work_dir

  def t(self, line):
    return precmd(cmd="time", line=line, cond=self.auto_time)

  def add_parallel(self, jobs):
    assert type(jobs) != str
    cmd = make_parallel(self.work_dir, self.jobname, jobs, self.auto_time)
    self.cmds.append(cmd)
    
  def add(self, job, simple=False):
    if simple:
      self.cmds.append(job)
    else:
      self.cmds.append(self.t(job))

  def echo(self, msg):
    self.cmds.append("echo %s" % msg)

  def qsub_script(self):
    script = "\n".join(self.cmds)
    return fill_template(jobname=self.jobname, n_nodes=self.n_nodes, n_ppn=self.n_ppn, walltime=self.walltime, options=self.options, script=script, work_dir=self.work_dir)

  def submit(self):
    qsub_script = self.qsub_script
    submit(qsub_script)
    return qsub_script

    
    

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

def fill_template(jobname=None, n_nodes=1, n_ppn=1, walltime='2:00:00', options="", script=None, work_dir=WORK_DIR, *vargs, **kwds):
  """Fill qsub submission script. Absorb any unrecognized keywords."""
  assert all((jobname, n_nodes, n_ppn, walltime, script))
  assert type(options) == str
  n_ppn = int(n_ppn)
  n_nodes = int(n_nodes)
  # possibly handle walltime estimates
  return TEMPLATE % locals()

def submit(script_txt):
  p = subprocess.Popen("qsub", stdin=subprocess.PIPE)
  p.communicate(input=script_txt)
  p.stdin.close()
  return True


def tstamp():
  return datetime.datetime.isoformat(datetime.datetime.now())

def make_script_name(work_dir, job_name):
  random.seed()
  tmp_script_name = "tmp_parallel_script_%s_%s_%d.sh" % \
      (job_name, tstamp(), random.randint(0,10000000))
  dispatch_script_fname = os.path.join(work_dir, tmp_script_name)
  return dispatch_script_fname

def precmd(cmd, line, cond):
  if cond and line.partition(' ')[0] != cmd:
    return " ".join(([cmd] + line.split(' ')))
  else:
    return line


if __name__ == "__main__":
  print fill_template(**dict([s.split('=') for s in sys.argv[1:]]))
