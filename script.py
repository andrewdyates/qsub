from __init__ import *


if __name__ == "__main__":
  submit(fill_template(**dict([s.split('=') for s in sys.argv[1:]])))
