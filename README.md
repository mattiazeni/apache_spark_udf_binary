# Compile and Run

The application is as self-contained as possible. You just need to have Java and Maven installed on your system. You then need to run:

```mvn clean install```

and:

```mvn exec:java```

That's it. This will create a local Apache Spark cluster, create a dummy ```DataSet``` of ```MyWonderfulClass``` objects and then run the newly created UDF on it.