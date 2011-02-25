#!/bin/bash

echo "Installing dependencies for SuperCSV"
mvn install:install-file -Dfile=src/main/3rdParty/spiffy-with_source-all-0.05.jar -DgroupId=spiffy -DartifactId=spiffy -Dversion=0.05 -Dpackaging=jar
mvn install:install-file -Dfile=src/main/3rdParty/SuperCSV-1.52.jar -DgroupId=supercsv -DartifactId=supercsv -Dversion=1.5.2 -Dpackaging=jar 

echo "making the project"
mvn clean compile package eclipse:eclipse

echo "All done! To run just type: run.sh"

