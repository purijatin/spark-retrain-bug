# spark-retrain-bug

This is a reproducible for a probable-spark bug.

```
To run: sbt -mem 1500 run
```

The program in a loop:

* Trains based on the data files found at: data/ folder. The data set is subset of data of amazon electronic dataset which can be found at: http://jmcauley.ucsd.edu/data/amazon/
* It simply calculates tf-idf of the terms after (Tokenizer -> Stopword remover -> stemming -> tf -> tfidf)
* Once the training is complete, it unpersists and starts the operation again.

After about 10 runs (takes ~20minutes), it ends with OOME. 

## Fix
This issue is not found if all the input files are merged to a single file.  
Can only be reproduced when each document is in a separate file.

## Heap dump details

The biggest objects in the heapdump:

![image](/objects.png)

Further debug:

![image](inside.png)

## Other details:

Spark 2.3.0
JDK8
SBT 1.1.6
Ubuntu 18.04
Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz

