#!/bin/bash

# This file should't be pushed to open-source repo
user=$1
# repository="project-taiga-docker-local"

if docker login -u svc.taiga.usr -p '7.bL.p6x3^2P^.YAXyu' project-taiga-docker-local.artifactory.eng.vmware.com; 
then
    echo "Login succeeded"
else
    echo "Login failed"
    exit 1
fi

if docker build  --no-cache -f docker/ray-on-vsphere-dev/Dockerfile . -t "project-taiga-docker-local.artifactory.eng.vmware.com/development/ray:$user";
then
    echo "Docker build succedded"
else
    echo "Docker build failed"
    exit 1
fi

if docker push "project-taiga-docker-local.artifactory.eng.vmware.com/development/ray:$user";
then
    echo "Docker push succeeded"
else
    echo "Docker push failed"
    exit 1
fi
