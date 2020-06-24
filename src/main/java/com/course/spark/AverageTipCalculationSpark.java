package com.course.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;

import static com.course.plainJava.FieldMapper.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class AverageTipCalculationSpark {

    private static final String EXECUTOR_MEMORY = "spark.executor.memory";
    private static final String EXECUTOR_CORES = "spark.executor.cores";
    private static final String EXECUTOR_INSTANCES = "spark.executor.instances";
    private static final String PATH_TO_FILES_WITH_OLD_SCHEMA = "old";
    private static final String PATH_TO_FILES_WITH_NEW_SCHEMA = "new";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        Map<String, String> parameters = parseParameters(args);
        SparkSession sparkSession = constructSparkSession(parameters);
        DataFrameReader dataFrameReader = sparkSession.read();

        Dataset<Row> tripRowFromNewSchema = readDSWithSelect(dataFrameReader, parameters.get(PATH_TO_FILES_WITH_NEW_SCHEMA));
        Dataset<Row> tripRowFromOldSchema = readDSWithSelect(dataFrameReader, parameters.get(PATH_TO_FILES_WITH_OLD_SCHEMA));

        Dataset<Row> mergedTripRow = tripRowFromNewSchema.union(tripRowFromOldSchema)
                .filter(col("paymentType").isNotNull());

        Dataset<TripTypedRow> tripItem = mergedTripRow
                .map((MapFunction<Row, TripTypedRow>) row ->
                        new TripTypedRow(
                                parseDateTime(row.<String>getAs("pickUpDateTime")),
                                parseDistance(row.<String>getAs("tripDistance")),
                                parsePayment(row.<String>getAs("paymentType")),
                                Float.parseFloat(row.<String>getAs("tip"))
                        ), Encoders.bean(TripTypedRow.class));

        tripItem.groupBy("pickUpDateTime", "tripDistance", "paymentType").agg(avg("tip")).show(100);


    }

    private static Map<String, String> parseParameters(String[] args) {
        HashMap<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String[] config = args[i].split("=");
            params.put(config[0], config[1]);
        }
        return params;
    }

    private static SparkSession constructSparkSession(Map<String, String> params) {

        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .appName("yellow-trip-data-calculation");

        if (params.size() > 2) {
            return sessionBuilder
                    .config(EXECUTOR_MEMORY, params.get(EXECUTOR_MEMORY))
                    .config(EXECUTOR_CORES, params.get(EXECUTOR_CORES))
                    .config(EXECUTOR_INSTANCES, params.get(EXECUTOR_INSTANCES))
                    .getOrCreate();
        }
        return sessionBuilder.master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> readDSWithSelect(DataFrameReader dataFrameReader, String path) {

        return dataFrameReader.option("header", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .csv(path)
                .select(
                        col("tpep_pickup_datetime").as("pickUpDateTime"),
                        col("trip_distance").as("tripDistance"),
                        col("payment_type").as("paymentType"),
                        col("tip_amount").as("tip")
                );
    }


}
