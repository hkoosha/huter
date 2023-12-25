FROM huter-hive3

LABEL maintainer="Koosha Hosseiny <i@koosha.io>"

RUN    sed -i 's/HuterMain/HuterRepoMain/g' /opt/huter/bin/huter-hive3

VOLUME "/repo"

WORKDIR "/repo"

CMD ["/usr/bin/huter", "/repo"]
