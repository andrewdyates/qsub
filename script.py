#!/usr/bin/python
"""Script handle.

EXAMPLE USE:
  python script.py script="echo 'hello'"
"""
from __init__ import *

if __name__ == "__main__":
  print submit(fill_template(**dict([(s.partition('=')[0], s.partition('=')[2]) for s in sys.argv[1:]])))
