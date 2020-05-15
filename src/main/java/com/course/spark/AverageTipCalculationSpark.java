package com.course.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static com.course.plainJava.FieldMapper.*;
import static org.apache.spark.sql.functions.avg;

public class AverageTipCalculationSpark {

    public static void main(String[] args) {
        String path = args[0];

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder().appName("sqlHousePriceSolution").master("local[3]").getOrCreate();

        DataFrameReader frameReader = sparkSession.read();

        Dataset<TripRow> tripItemRow = frameReader
                .option("header", "true")
                .option("mode", "FAILFAST")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
//                .schema(Encoders.bean(TripRow.class).schema())
                .csv(path)
                .as(Encoders.bean(TripRow.class))
                .filter(row -> Optional.ofNullable(row.getPayment_type()).isPresent());

        Dataset<TripTypedRow> tripItem = tripItemRow
                .map(row ->
                        new TripTypedRow(
                                parseDateTime(row.getTpep_pickup_datetime()),
                                parseDistance(row.getTrip_distance()),
                                parsePayment(row.getPayment_type()),
                                Float.parseFloat(row.getTip_amount())), Encoders.bean(TripTypedRow.class));

        tripItem.groupBy("pickUpDateTime", "tripDistance", "paymentType").agg(avg("tip")).show(100);


    }
}
