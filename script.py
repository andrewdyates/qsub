#!/usr/bin/python
"""Script handle.

EXAMPLE USE:
  python script.py script="echo 'hello'" options="#PBS -M yates.115.osu@gmail.com"
"""
import sys
from __init__ import *


def main():
  kwds = dict([(s.partition('=')[0], s.partition('=')[2]) for s in sys.argv[1:]])
  cmd = kwds['script']
  dry = kwds.get('dry', False)
  if dry in ('f','false','False','None'): dry = False
  del kwds['script']
  if 'dry' in kwds: del kwds['dry']
  Q = Qsub(**kwds)
  Q.add(cmd)
  print Q.script()
  print Q.submit(dry)
  
if __name__ == "__main__":
  main()
