FROM docker.io/library/python:3.14-alpine
LABEL zimfarm=true
LABEL org.opencontainers.image.source=https://github.com/kiwix/uploader

ENV HOST_KNOW_FILE=/etc/ssh/known_hosts
ENV MARKER_FILE=/usr/share/marker
ENV SCP_BIN_PATH=/usr/bin/scp
ENV SFTP_BIN_PATH=/usr/bin/sftp

COPY pyproject.toml README.md LICENSE /src/
COPY src/kiwix_uploader/__init__.py /src/src/kiwix_uploader/__init__.py

RUN apk add openssh \
    && pip install --no-cache-dir /src

COPY src /src/src
COPY *.md /src/

RUN pip install --no-cache-dir /src \
    && rm -rf /src \
    && touch /usr/share/marker


CMD ["uploader", "--help"]
