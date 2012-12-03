#!/usr/name/python
"""Qsub job dispatcher.

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
import subprocess
import random
import datetime
import os, errno

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
source $HOME/.bash_profile
echo "PBS_JOBID: $PBS_JOBID"
%(script)s
"""
# Default work directory is user's home directory
WORK_DIR = os.environ["HOME"]
MAX_PPN_OAKLEY = 12
MAX_PPN_GLENN = 8

def get_mail_option(begin=False, end=True, abort=True):
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

def get_delay_option(hour, minute=00, year=None, month=None, day=None, seconds=None):
  if year: assert month
  if month: assert day
  s = filter(lambda x: x is not None, (year, month, day, hour, minute, seconds))
  date_time = "".join(["%d"%x for x in s])
  return "#PBS -a %s" % date_time

def get_stderr_option(path):
  return "#PBS -e %s" % (path)
def get_stdout_option(path):
  return "#PBS -o %s" % (path)

def get_depend_option(jobids, rel="after", exetype="any"):
  """Handles simplest case of dependency. See qsub manual for more complex options."""
  return "#PBS -W depend=%s%s:%s" % (rel, exetype, ":".join(jobids))

class Qsub(object):
  """Simple wrapper for qsub job building functionality."""
  def __init__(self, jobname="untitled", n_nodes=1, n_ppn=1, hours=1, minutes=0, seconds=0, options=None, work_dir=WORK_DIR, auto_time=True, email=False, stdout_fpath=None, stderr_fpath=None, after_jobids=None):
    self.jobname = jobname
    self.n_nodes = n_nodes
    self.n_ppn = n_ppn
    self.walltime = timestr(hours, minutes, seconds)
    if options is None:
      self.options = []
    else:
      self.options = options
    self.work_dir = work_dir
    self.auto_time = auto_time
    self.cmds = []
    if email:
      self.options.append(get_mail_option())
    if stdout_fpath:
      self.options.append(get_stdout_option(stdout_fpath))
    if stderr_fpath:
      self.options.append(get_stderr_option(stderr_fpath))
    if after_jobids:
      if type(after_jobids) == str:
        after_jobids.split(':')
      self.options.append(get_depend_option(after_jobids))
      
  def t(self, line):
    return precmd(cmd="/usr/bin/time", line=line, cond=self.auto_time)

  def add_parallel(self, jobs):
    assert type(jobs) != str
    cmd = make_parallel(self.work_dir, self.jobname, jobs, self.auto_time)
    self.cmds.append(cmd)
    
  def add(self, job, simple=False, pernode=None):
    if job is None:
      import sys
      print "!!!!"
      sys.exit(1)
    if not simple:
      job = self.t(job)
    if pernode is not None:
      assert pernode <= self.n_ppn and pernode >= 1
      precmd(cmd="time", line=job, cond=self.auto_time)
      job = precmd("mpiexec -npernode %d " % pernode, job)
    self.cmds.append(job)

  def echo(self, msg):
    self.cmds.append("echo %s" % msg)

  def script(self):
    """Return current qsub script that will be submitted."""
    script = "\n".join(self.cmds)
    options = "\n".join(self.options)
    return fill_template(jobname=self.jobname, n_nodes=self.n_nodes, n_ppn=self.n_ppn, walltime=self.walltime, options=options, script=script, work_dir=self.work_dir)

  def submit(self, dry=False):
    """Submit qsub script, return job ID."""
    if not dry:
      qsub_script = self.script()
      return submit(qsub_script)
    else:
      return "DRYRUN"
    
    

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

def fill_template(jobname="untitled", n_nodes=1, n_ppn=1, walltime='0:40:00', options="", script=None, work_dir=WORK_DIR, *vargs, **kwds):
  """Fill qsub submission script. Absorb any unrecognized keywords."""
  assert True or vargs is None or kwds is None # thwart pychecker warnings
  assert all((jobname, n_nodes, n_ppn, walltime, script, work_dir)), str((jobname, n_nodes, n_ppn, walltime, script, work_dir))
  assert type(options) == str
  n_ppn = int(n_ppn)
  n_nodes = int(n_nodes)
  return TEMPLATE % locals()

def submit(script_txt):
  """Submit qsub script, return job ID."""
  p = subprocess.Popen("qsub", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, stderr = p.communicate(input=script_txt)
  p.stdin.close()
  if stderr:
    raise Exception, stderr
  return stdout.strip('\n')


def tstamp():
  return datetime.datetime.isoformat(datetime.datetime.now())

def make_script_name(work_dir, job_name):
  random.seed()
  tmp_dir = os.path.join(work_dir, "tmp_scripts")
  if not os.path.exists(tmp_dir):
    make_dir(tmp_dir)
  tmp_script_name = "tmp_parallel_script_%s_%s_%d.sh" % \
      (job_name, tstamp(), random.randint(0,10000000))
  dispatch_script_fname = os.path.join(tmp_dir, tmp_script_name)
  return dispatch_script_fname

def precmd(cmd, line, cond=True):
  if cond and line.partition(' ')[0] != cmd:
    return " ".join(([cmd] + line.split(' ')))
  else:
    return line

def make_dir(outdir):
  try:
    os.makedirs(outdir)
  except OSError, e:
    if e.errno != errno.EEXIST: raise
  return outdir
