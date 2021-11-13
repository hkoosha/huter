FROM openjdk:8
LABEL maintainer="Koosha Hosseiny <koosha.hosseiny@trivago.com>"

COPY huter /opt/huter
COPY huter-bin /tmp

RUN    cd /opt \
    && mv /tmp/huter-bin /usr/bin/huter \
    && chmod +x /usr/bin/huter

VOLUME "/repo"

WORKDIR "/repo"

CMD ["/bin/bash"]
