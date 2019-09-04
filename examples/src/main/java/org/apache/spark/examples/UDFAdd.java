package org.apache.spark.examples;

//import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFAdd /*extends UDF */{

    public Double evaluate(Double ...a) {
        Double total = 0.0;
        for (Double i: a) {
            total += i.doubleValue();
        }
        return total;
    }
}
