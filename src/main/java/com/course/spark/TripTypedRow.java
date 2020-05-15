package com.course.spark;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class TripTypedRow implements Serializable {

    private String pickUpDateTime;
    private String tripDistance;
    private String paymentType;
    private Float tip;
}
