#!/bin/bash
#SBATCH --job-name=index
#SBATCH --output=/EDISON/SCIDATA/sdr/tmp/vcf/std.out
#SBATCH --error=/EDISON/SCIDATA/sdr/tmp/vcf/std.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --chdir=/EDISON/SCIDATA/sdr/tmp/vcf
JOBDIR=/EDISON/SCIDATA/sdr/tmp/vcf
USER=kwon
SINDIR=/EDISON/SCIDATA/sdr/singularity-images
source /usr/lib64/anaconda3/etc/profile.d/conda.sh

/usr/bin/singularity exec -H ${JOBDIR}:/home/kwon -B /EDISON/SCIDATA/sdr/tmp/vcf:/tmp/vcf --pwd ${JOBDIR} /EDISON/SCIDATA/singularity-images/userenv3 python /tmp/vcf/vcfcode/main.py /EDISON/SCIDATA/sdr/tmp/vcf/1000genome /EDISON/SCIDATA/sdr/tmp/vcf/targetdir 

