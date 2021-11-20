#!/bin/bash
#SBATCH --chdir /scratch/cassayre
#SBATCH --nodes 1
#SBATCH --ntasks 1
#SBATCH --cpus-per-task 8
#SBATCH --mem 40G
#SBATCH --time 03:00:00

echo Start: `date +"%Y-%m-%d %H:%M:%S"`

bash /home/cassayre/toolbox/dispatch_notification.sh "Task ${SLURM_JOB_ID} (INSEE) started on $(date)"

java -cp /home/cassayre/insee/insee-db-assembly-0.1.jar MainWrite /scratch/cassayre /home/cassayre/insee/data

bash /home/cassayre/toolbox/dispatch_notification.sh "Task ${SLURM_JOB_ID} (INSEE) completed on $(date)"

echo Finish: `date +"%Y-%m-%d %H:%M:%S"`
