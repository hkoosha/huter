DOCKER_LABEL := "huter-hive3-pytest"

CONFTEST_FILE := conftest.py
SAMPLE_FILE := test_sample_script.py

include ../Makefile.common

.PHONY: get-files
get-files: clean
	cp ../../python/${CONFTEST_FILE} .
	cp ../../python/${SAMPLE_FILE} .

.PHONY: clean
clean:
	sudo rm -rf __pycache__
	sudo rm -rf .pytest_cache
	rm -rf ${CONFTEST_FILE}
	rm -rf ${SAMPLE_FILE}
