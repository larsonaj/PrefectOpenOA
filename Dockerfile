FROM prefecthq/prefect:latest-python3.8

WORKDIR /usr/src/app

ADD . .


RUN python3 -m pip download Bottleneck setuptools numpy==1.13.3 wheel -d ./
RUN pip install .
CMD prefect backend cloud ; prefect auth login --key "$PREFECT_API_KEY" ; prefect agent local start
EXPOSE 5000
