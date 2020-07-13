package com.course.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import static com.course.utils.FieldMapper.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class AverageTipCalculationSimple {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        String path = "";

        SparkSession sparkSession = SparkSession.builder()
                .appName("yellow-trip-data-calculation")
                .master("local[*]")
                .getOrCreate();

        DataFrameReader dataFrameReader = sparkSession.read();

        Dataset<Row> filtered = dataFrameReader.option("header", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .csv(path)
                .select(
                        col("tpep_pickup_datetime").as("pickUpDateTime"),
                        col("trip_distance").as("tripDistance"),
                        col("payment_type").as("paymentType"),
                        col("tip_amount").as("tip")
                )
                .filter(col("paymentType").isNotNull());

        Dataset<TripTypedRow> tripItem = filtered.map((MapFunction<Row, TripTypedRow>) row ->
                new TripTypedRow(
                        mapDateTime(row.<String>getAs("pickUpDateTime")),
                        mapDistance(row.<String>getAs("tripDistance")),
                        mapPayment(row.<String>getAs("paymentType")),
                        Float.parseFloat(row.<String>getAs("tip"))
                ), Encoders.bean(TripTypedRow.class));

        tripItem.groupBy("pickUpDateTime", "tripDistance", "paymentType").agg(avg("tip")).show(100);
    }
}
