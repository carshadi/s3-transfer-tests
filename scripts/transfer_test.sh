#!/bin/bash
#SBATCH --job-name=s3-transfer-manager-test
#SBATCH --output=/allen/scratch/aindtemp/cameron.arshadi/job-logs/s3-transfer-manager-test_%j.log
#SBATCH --partition aind
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=04:00:00
#SBATCH --array=1-8

export JAVA_HOME="/allen/scratch/aindtemp/cameron.arshadi/tools/jvm/zulu8.62.0.19-ca-jdk8.0.332-linux_x64"
export PATH=$JAVA_HOME/bin:$PATH

JAR=/allen/programs/aind/workgroups/msma/cameron.arshadi/tools/s3-transfer-tests/target/s3-transfer-tests.jar
MAIN_CLASS=org.aind.msma.S3TransferManagerTest

PER_TASK=11

START_INDEX=$(( ($SLURM_ARRAY_TASK_ID - 1) * $PER_TASK ))

if (( $SLURM_ARRAY_TASK_ID == 8 )); then
  # process the remainder
  END_INDEX=-1
else
  END_INDEX=$(( $SLURM_ARRAY_TASK_ID * $PER_TASK ))
fi

echo This is task $SLURM_ARRAY_TASK_ID, which will do runs $START_INDEX to $END_INDEX

java -cp $JAR $MAIN_CLASS \
--inputFolder /allen/programs/aind/workgroups/msma/test-file.zarr \
--bucket $BUCKET \
--prefix java-sdk-zarr-transfer-test \
--targetThroughput 100.0 \
--minPartSize 128000000 \
--startIndex $START_INDEX \
--endIndex $END_INDEX