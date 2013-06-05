#!/usr/bin/python
"""Script handle.

EXAMPLE USE:
  python script.py script="echo 'hello'" options="#PBS -M yates.115.osu@gmail.com"
"""
import sys
from __init__ import *


def main():
  kwds = dict([(s.partition('=')[0], s.partition('=')[2]) for s in sys.argv[1:]])
  script = fill_template(**kwds)
  print script
  print submit(script)
  
if __name__ == "__main__":
  main()
