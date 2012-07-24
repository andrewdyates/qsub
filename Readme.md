# qsub job submitter

Submit jobs to the super computing queue dynamically. Intended for use on the Oakley super computing cluster at the Ohio Supercomputing Center. In Python.

EXAMPLE USE:

``from qsub import *
script_txt= "echo 'hello, world'"
qsub_txt = fill_template(jobname="hellworld", script=script_txt)
print "Submitting..."
print qsub_txt
submit(qsub_txt)``