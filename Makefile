.PHONY: publish-local
publish-local:
	./gradlew publishToMavenLocal

.PHONY: clean
clean:
	./gradlew clean
	cd docker && $(MAKE) clean

.PHONY: build
build:
	./gradlew build -xdistTar -xdistZip

.PHONY: dist
dist:
	./gradlew build -xdistZip
