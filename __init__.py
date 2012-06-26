#!/usr/name/python
import sys
TEMPLATE = \
"""#PBS -N %(jobname)s
#PBS -l nodes=%(n_nodes)d:ppn=%(n_ppn)d
#PBS -j oe
#PBS -S /bin/bash
#PBS -l walltime=%(walltime)s
%(options)s
#tdate=$(date +%%T)

set -x
cd /nfs/01/osu6683/
source .bash_profile
%(script)s
"""

def timestr(hours=0, minutes=0, seconds=0):
  return "%d:%.2d:%.2d" % (hours, minutes, seconds)

def fill_template(jobname='qsub.py_untitled_run', n_nodes=1, n_ppn=1, walltime='2:00:00', options="", script=None, *vargs, **kwds):
  """Fill qsub submission script. Absorb any unrecognized keywords."""
  assert all((jobname, n_nodes, n_ppn, walltime, script))
  assert type(options) == str
  n_ppn = int(n_ppn)
  n_nodes = int(n_nodes)
  # possibly handle walltime estimates
  return TEMPLATE % locals()

if __name__ == "__main__":
  print fill_template(**dict([s.split('=') for s in sys.argv[1:]]))
