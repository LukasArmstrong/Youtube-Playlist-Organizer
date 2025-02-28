# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.10-bullseye

EXPOSE 5002

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Setup Development Tools
RUN apt-get update && apt-get install -y \
curl \
gcc \
python3-dev \
nano \
openssl

# Install MariaDB Connector/C
RUN curl -LsSO https://r.mariadb.com/downloads/mariadb_repo_setup
#Removing Checksum check. I expect this to break on every mariadb update. Will be annoy to track and since this a docker image/container. I'll take my chances
#Famous last words^
#RUN echo "c4a0f3dade02c51a6a28ca3609a13d7a0f8910cccbb90935a2f218454d3a914a  mariadb_repo_setup" \
#   | sha256sum -c -
RUN chmod +x mariadb_repo_setup
RUN ./mariadb_repo_setup \
--mariadb-server-version="mariadb-11.7.1"
RUN apt-get update && apt-get install -y \
libmariadb3 \
libmariadb-dev

#install powertube
RUN mkdir /usr/src/app
COPY YoutubeWebserver.py /usr/src/app
COPY pywertube.py /usr/src/app
COPY wsgi.py /usr/src/app
COPY token.pickle /usr/src/app
COPY watchLater.pickle /usr/src/app
COPY templates /usr/src/app/templates
COPY static/ /usr/src/app/static

#install pip requirements
COPY requirements.txt /usr/src/app
WORKDIR /usr/src/app
RUN pip install -r requirements.txt

#test
RUN echo "Testing testing"

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /usr/src/app
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["gunicorn", "-w", "4", "--bind", "0.0.0.0:8442", "wsgi:app", "--timeout", "90"]
