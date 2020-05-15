package com.course.plainJava;


import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class Trip {

    private String pickUpDateTime;
    private String tripDistance;
    private String paymentType;

}
