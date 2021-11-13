DOCKER_LABEL := "huter-hive2"

HUTER_FILE := huter-hive2-1.0
HUTER_ARCHIVE := "${HUTER_FILE}.tar"
HUTER_FROM := "../../huter-hive2/build/distributions/${HUTER_ARCHIVE}"

include ../Makefile.common

.PHONY: get-files
get-files: clean
	cp "${HUTER_FROM}" .
	tar xf ${HUTER_ARCHIVE}
	mv ${HUTER_FILE} huter
	rm -rf ${HUTER_ARCHIVE}

.PHONY: clean
clean:
	rm -rf ${HUTER_ARCHIVE}
	rm -rf ${HUTER_FILE}
	rm -rf huter

