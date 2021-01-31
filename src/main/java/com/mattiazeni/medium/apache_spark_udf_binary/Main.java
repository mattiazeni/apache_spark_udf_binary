package com.mattiazeni.medium.apache_spark_udf_binary;

import com.mattiazeni.medium.apache_spark_udf_binary.model.MyWonderfulClass;
import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import sun.rmi.runtime.Log;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mattiazeni on 04/06/2018.
 */

public class Main {
    public static void main(String[] args) {

        SparkSession sparkSession = createSparkSession();

        Dataset<MyWonderfulClass> inputDataset = generateDummyInputDataset(10, sparkSession);
        inputDataset.show();

        Dataset<Row> processedData = processDataset(inputDataset);
        processedData.show();
    }

    private static SparkSession createSparkSession() {
        SparkSession.Builder builder = new SparkSession.Builder();
        builder = builder.master("local[*]");
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1");

        SparkSession sparkSession = builder.getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        UDFRegistration udfRegistration = sparkSession.sqlContext().udf();
        udfRegistration.register("extractIdUDF", extractIdUDF(), DataTypes.LongType);

        return sparkSession;
    }

    private static UDF1<byte[], Long> extractIdUDF() {
        return (byteArray) -> {
            if (byteArray != null) {
                ByteArrayInputStream in = new ByteArrayInputStream(byteArray);
                ObjectInputStream is = new ObjectInputStream(in);
                MyWonderfulClass myWonderfulClass = (MyWonderfulClass) is.readObject();
                return myWonderfulClass.getId();
            }
            else {
                return -1L;
            }
        };
    }

    private static Dataset<Row> processDataset(Dataset<MyWonderfulClass> data) {
        return data
                .withColumn( "ID", functions.callUDF( "extractIdUDF", new Column("value")))
                .drop("value");
    }

    private static Dataset<MyWonderfulClass> generateDummyInputDataset(int size, SparkSession sparkSession) {
        List<MyWonderfulClass> list = new ArrayList<MyWonderfulClass>();

        for(int index=0; index < size; index++) {
            list.add(new MyWonderfulClass());
        }

        return sparkSession.createDataset(list, Encoders.javaSerialization(MyWonderfulClass.class));
    }
}