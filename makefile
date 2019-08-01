clean:
	sbt clean
	./clean.sh

lint:
	sbt clean scalastyle test:scalastyle scapegoat scalafmtCheckAll test


build:
	sbt clean scalastyle test:scalastyle scapegoat scalafmtCheckAll assembly


release: build
	./clean.sh
	./release-jar.sh
