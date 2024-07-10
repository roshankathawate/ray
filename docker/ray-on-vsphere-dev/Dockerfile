FROM rayproject/ray:latest-py38-cpu

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN pip install --no-compile --no-cache-dir --upgrade "ray[default] @https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl" \
    && pip install --no-compile --no-cache-dir --upgrade grpcio==1.54.2 kubernetes

COPY --chown=ray:users . ray

RUN python ray/python/ray/setup-dev.py \
    --skip rllib air tune train cloudpickle data internal tests experimental util workflow dag widgets cluster_utils.py _private dashboard scripts serve \
    -y