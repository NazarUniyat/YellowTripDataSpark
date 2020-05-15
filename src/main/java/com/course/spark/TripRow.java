package com.course.spark;


import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class TripRow implements Serializable {
    private String VendorID;
    private String tpep_pickup_datetime;
    private String tpep_dropoff_datetime;
    private String passenger_count;
    private String trip_distance;
    private String RatecodeID;
    private String store_and_fwd_flag;
    private String PULocationID;
    private String DOLocationID;
    private String payment_type;
    private String fare_amount;
    private String extra;
    private String mta_tax;
    private String tip_amount;
    private String tolls_amount;
    private String improvement_surcharge;
    private String total_amount;
//    private String congestion_surcharge;
}
