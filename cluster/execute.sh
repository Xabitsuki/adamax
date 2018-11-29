#!/usr/bin/env bash
USER=$1
SCRIPT=$2

scp $SCRIPT ${USER}@iccluster028.iccluster.epfl.ch:/home/${USER}

# send python script ot run cluster
# scp script rubiato@iccluster028.iccluster.epfl.ch:/home/rubiato

# submit job
ssh ${USER}@iccluster028.iccluster.epfl.ch spark-submit --master yarn \
                                                        --deploy-mode client \
                                                        --driver-memory 4G \
                                                        --num-executors 5 \
                                                        --executor-memory 4G \
                                                        --executor-cores 5 \
                                                        ${SCRIPT}



spark-submit --master yarn --deploy-mode cluster --driver-memory 4G --num-executors 5 --executor-memory 4G --executor-cores 5 ezcluster.py