.PHONY: publish-local
publish-local:
	./gradlew publishToMavenLocal

.PHONY: clean
clean:
	./gradlew --warning-mode all clean
	cd docker && $(MAKE) clean

.PHONY: build
build:
	./gradlew --warning-mode all build -xdistTar -xdistZip

.PHONY: dist
dist:
	./gradlew --warning-mode all build -xdistZip

