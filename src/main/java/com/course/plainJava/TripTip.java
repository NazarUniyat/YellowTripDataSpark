package com.course.plainJava;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class TripTip {

    private Trip trip;
    private Double tip;
}
