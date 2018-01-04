These are example scripts for executing an automated regression test on the 
Theta system at the ALCF.  The entire process is handled by the 
"run-regression.sh" script, which is suitable for execution within a cron job.

The "run-regression-static.sh" script is a variation on the above that does
an entirely static build of all executables.  For now it includes a few
workarounds for linking problems in some of the dependencies.
