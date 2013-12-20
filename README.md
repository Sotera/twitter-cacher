TwitterCacher2
==============
Twitter Scraper

Pre-requisites:
Java
Maven

Building:
Change to project directory (contains pom.xml)
Execute "mvn install" in console
This produces two jar files in the target directory (e.g. TwitterCacher2-0.0.1-SNAPSHOT.jar and TwitterCacher2-0.0.1-SNAPSHOT-jar-with-dependencies.jar)

Running:
"java -jar TwitterCacher2-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
This will create a /data/ folder and load all records into .tsv files in this location