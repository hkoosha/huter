FROM huter-hive2

LABEL maintainer="Koosha Hosseiny <koosha.hosseiny@trivago.com>"

ENV DEBIAN_FRONTEND noninteractive

RUN    apt-get update \
    && apt-get install --no-install-recommends -y python3 python3-pip \
    && pip3 install py4j pytest \
    && apt-get clean all -y \
    && mkdir /sample

COPY get_conftest_py /usr/bin/conftest_file
COPY get_conftest_py_content /usr/bin/conftest
COPY get_test_sample_script_content /usr/bin/sample
COPY conftest.py /opt/huter/
COPY test_sample_script.py /sample/

RUN    chmod +x /usr/bin/conftest_file \
    && chmod +x /usr/bin/conftest \
    && chmod +x /usr/bin/sample

VOLUME "/repo"

WORKDIR "/repo"

CMD ["/usr/local/bin/pytest", "/repo"]
