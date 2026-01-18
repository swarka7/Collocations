# Collocations (EMR)

MapReduce pipeline for extracting top-100 collocations per decade (Hebrew + English) using LLR.

## Build
```
mvn -DskipTests package
```

## Run (local orchestrator)
Example (Hebrew only, combiner-only counts):
```
java -cp target\\collocations-1.0-SNAPSHOT.jar collocations.EmrFlowRunner --jar target\\collocations-1.0-SNAPSHOT.jar --bucket <your-bucket> --logUri s3://<your-bucket>/hw2/emr-logs/ --runLangs he --stopwordsHe heb-stopwords.txt --instanceType m4.large --instanceCount 2 --countsMode combinerOnly
```

Example (both languages, include no-combiner counts run):
```
java -cp target\\collocations-1.0-SNAPSHOT.jar collocations.EmrFlowRunner --jar target\\collocations-1.0-SNAPSHOT.jar --bucket <your-bucket> --logUri s3://<your-bucket>/hw2/emr-logs/ --runLangs he,en --stopwordsHe heb-stopwords.txt --stopwordsEn eng-stopwords.txt --instanceType m4.large --instanceCount 2 --countsMode both
```

## Outputs
Outputs are written to:
```
s3://<your-bucket>/collocations/<lang>/<runId>/
```
Final results are in:
```
s3://<your-bucket>/collocations/<lang>/<runId>/job5-top100/part-r-00000
```

