FROM python:3.10.6-buster
#COPY ./code /srv/
RUN pip install --upgrade pip && \
    pip install requests protobuf google-auth google-cloud-core google-cloud-translate google-cloud-vision
WORKDIR /srv
CMD ["python", "/srv/run.py"]
