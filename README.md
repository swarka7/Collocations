# Collocations (EMR)

End-to-end MapReduce pipeline that extracts the top-100 collocations per decade
for Hebrew and English using log-likelihood ratio (LLR). The pipeline runs on
Amazon EMR and is orchestrated locally by a standalone runner that uploads the
jar, creates a cluster, and submits steps in order.

## Overview
- Inputs: Google N-grams 2-gram corpora (SequenceFileInputFormat).
- Filtering: removes bigrams containing stopwords (heb/eng lists in resources).
- Output: top-100 bigrams per (language, decade) sorted by LLR, written to S3.
- No in-memory assumptions: joins are streaming reduce-side; top-100 uses bounded heap.

## Pipeline (per language)
1) Counts (with combiner): emit C12, C1, C2, and N per decade.
2) Join by w1: attach C1 to each (w1,w2).
3) Join by w2: attach C2 to each (w1,w2).
4) LLR: load N per decade and compute LLR for each bigram.
5) Top-100: keep highest LLR per decade.

## Inputs
- Hebrew 2-gram: `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data`
- English 2-gram (GB): `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data`

## Build
```
mvn -DskipTests package
```

## Run (local orchestrator)
Hebrew only (faster, combiner counts only):
```
java -cp target\\collocations-1.0-SNAPSHOT.jar collocations.EmrFlowRunner --jar target\\collocations-1.0-SNAPSHOT.jar --bucket <your-bucket> --logUri s3://<your-bucket>/hw2/emr-logs/ --runLangs he --stopwordsHe heb-stopwords.txt --instanceType m4.large --instanceCount 2 --countsMode combinerOnly
```

Both languages (include no-combiner counts run for counters report):
```
java -cp target\\collocations-1.0-SNAPSHOT.jar collocations.EmrFlowRunner --jar target\\collocations-1.0-SNAPSHOT.jar --bucket <your-bucket> --logUri s3://<your-bucket>/hw2/emr-logs/ --runLangs he,en --stopwordsHe heb-stopwords.txt --stopwordsEn eng-stopwords.txt --instanceType m4.large --instanceCount 2 --countsMode both
```

## Outputs
All outputs are written to:
```
s3://<your-bucket>/collocations/<lang>/<runId>/
```

Final results:
```
s3://<your-bucket>/collocations/<lang>/<runId>/job5-top100/part-r-00000
```

## Notes
- `--countsMode combinerOnly` skips the no-combiner counts job to reduce runtime.
- Counters report for job1 counts is saved as `counters-report.txt` in the counts output folder.
- EMR logs are written to the `--logUri` prefix and include step syslogs.
