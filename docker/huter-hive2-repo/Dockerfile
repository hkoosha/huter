FROM huter-hive2

LABEL maintainer="Koosha Hosseiny <koosha.hosseiny@trivago.com>"

RUN    sed -i 's/HuterMain/HuterRepoMain/g' /opt/huter/bin/huter-hive2

VOLUME "/repo"

WORKDIR "/repo"

CMD ["/usr/bin/huter", "/repo"]
