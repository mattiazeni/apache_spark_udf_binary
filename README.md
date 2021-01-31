# What is this
This is an exercise about creating a UDF in Java for Apache Spark to read a column of type binary in a Dataframe that encodes a Java Class. More details can be found in this [Medium article](https://mattiazeni.medium.com/apache-spark-udfs-with-binary-type-encoding-a-java-class-cb5fb158318). 

# Compile and Run

The application is as self-contained as possible. You just need to have Java and Maven installed on your system. You then need to run:

```mvn clean install```

and:

```mvn exec:java```

That's it. This will create a local Apache Spark cluster, create a dummy ```DataSet``` of ```MyWonderfulClass``` objects and then run the newly created UDF on it.
