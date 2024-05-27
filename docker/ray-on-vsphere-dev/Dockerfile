FROM rayproject/ray:latest-py38-cpu

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# ENV SVC_ACCOUNT_TOKEN=eyJhbGciOiJSUzI1NiIsImtpZCI6ImFrbFBlUlpLeld6YXpFTHZPbmp2REU0SGw2UnhNdi0xX0lwWHVTWWhuMTAifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNzE0NjgzNjQ1LCJpYXQiOjE3MTQwMzU2NDUsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJyb3NoYW4tZGVwbG95Iiwic2VydmljZWFjY291bnQiOnsibmFtZSI6InJrLWNsdXN0ZXIiLCJ1aWQiOiI3MTI5ZDk0ZS0xNjRhLTQ0ZTQtOGQzMC0zNzVlZmNmNGE0YTQifX0sIm5iZiI6MTcxNDAzNTY0NSwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50OnJvc2hhbi1kZXBsb3k6cmstY2x1c3RlciJ9.Gqj4LQVVv4_g6TFKSQ5SQ_a9X8x1YljkTG6cKafuQ5dfBQPBLLh5wkcPCPaeZPC6ZlnzDkL-sWCkMy2lIf84a7YaEERZZwACO9bH7-fC_TiVEWWYhtBJXWb1xa3TmNT5xb4q0bdyzvabjJ_yN_D_UufERzmzd7lskowxpdHvkjUan2TxltsoUuLGTJn5cVmA2Et0jGPsrd5c0l-9OvZT-11sugST6XNq6mSXA-z_FHz1qnUfvsPBImgb3POjfrIQqbThY8VoEcKI6CiBEMHIoMjNVz5rhGAtgs0QZYccZdn8f8bTFr3hafTTYqh0faBA_xx5-YSQ3H9MLU4Cc45yIA

RUN pip install --no-compile --no-cache-dir --upgrade "ray[default] @https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl" \
    && pip install --no-compile --no-cache-dir --upgrade grpcio==1.54.2 && pip install pytest

COPY --chown=ray:users . ray

RUN python ray/python/ray/setup-dev.py \
    --skip rllib air tune train cloudpickle data internal tests experimental util workflow dag widgets cluster_utils.py _private dashboard scripts serve \
    -y