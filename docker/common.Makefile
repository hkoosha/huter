SOURCE_REPO ?= $(shell pwd)/../../../../hadoop-wf

DOCKER_WORKDIR ?= /repo

.PHONY: build
build: get-files
	docker build -t ${DOCKER_LABEL} .

.PHONY: push
push:
	docker push ${DOCKER_LABEL}

.PHONY: run
run:
	docker run --rm -it \
		-v ${SOURCE_REPO}:${DOCKER_WORKDIR} \
		${DOCKER_LABEL}

.PHONY: attach
attach:
	docker run --rm -it \
		-v ${SOURCE_REPO}:${DOCKER_WORKDIR} \
		${DOCKER_LABEL} \
		/bin/bash
