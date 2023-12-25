DOCKER_LABEL := "huter-hive3-pytest"

CONFTEST_FILE := conftest.py
SAMPLE_FILE := test_sample_script.py

include ../common.Makefile

.PHONY: get-files
get-files: clean
	cp ../../python/${CONFTEST_FILE} .
	cp ../../python/${SAMPLE_FILE} .

.PHONY: clean
clean:
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf ${CONFTEST_FILE}
	rm -rf ${SAMPLE_FILE}
