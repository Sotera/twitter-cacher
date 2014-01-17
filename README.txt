twitter-cacher
==============
Twitter Scraper

Pre-requisites:
--------------------
Java
Maven

Building:
--------------------
Change to project directory (contains pom.xml)
Execute "mvn install" in console
This produces the files you will need in the target directory (TwitterCacher2-0.0.1-SNAPSHOT-jar-with-dependencies.jar, config.properties, profiles directory).

Configuration:
--------------------
The config.properties file is where you need to set your OAuth keys/tokens.
It also has lets you set where the program will place output files (outputDir).

Running:
--------------------
Make sure the jar, config file, and profiles directory are in the same location.
Execute "java -jar TwitterCacher2-0.0.1-SNAPSHOT-jar-with-dependencies.jar".
This will create your output folder if it does not exist and load all records into .tsv files in this location.
