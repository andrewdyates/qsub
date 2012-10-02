# qsub job submitter

Submit jobs to the super computing queue dynamically. Intended for use on the Oakley super computing cluster at the Ohio Supercomputing Center. In Python. Supports many parameters; see source code of __init__.py for details.

EXAMPLE USES:

*Using functions*
    from qsub import *
    script_txt= "echo 'hello, world'"
    qsub_txt = fill_template(jobname="hellworld", script=script_txt)
    print "Submitting..."
    jobid = submit(qsub_txt)
    
*Using job builder object*
    Q = Qsub(n_nodes=1, email=True)
    Q.add("mycmd a=b")
    Q.add_parallel(["python batch.py 1", "python batch.py 2"])
    Q.add("mycmd a=c")
    job_id = Q.submit()
