package com.mattiazeni.medium.apache_spark_udf_binary.model;

import org.apache.hadoop.io.BytesWritable;

import java.io.Serializable;
import java.util.Random;

public class MyWonderfulClass extends BytesWritable implements Serializable {

    private Long id;

    public MyWonderfulClass(Long id) {
        this.id = id;
    }

    public MyWonderfulClass() {
        Random random = new Random();
        this.id = random.nextLong();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
}
