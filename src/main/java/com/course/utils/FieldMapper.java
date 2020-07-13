package com.course.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class FieldMapper {

    public String mapDateTime(String dateToParse) {
        int hour = Integer.parseInt(dateToParse.substring(11, 13));
        String time = "Night";
        if (hour > 5 && hour < 12)
            time = "Morning";
        else if (hour >= 12 && hour < 17)
            time = "Afternoon";
        else if (hour >= 17 && hour < 21)
            time = "Evening";
        return time;
    }

    public String mapDistance(String distanceToParse) {
        float parsedDistance = Float.parseFloat(distanceToParse);
        String distance = "Short";
        if (parsedDistance > 2 && parsedDistance < 20)
            distance = "Medium";
        else if (parsedDistance >= 20)
            distance = "Long";
        return distance;
    }

    public String mapPayment(String paymentToParse) {
        int parsedPaymentType = Integer.parseInt(paymentToParse);
        String paymentType = "Credit card";
        if (parsedPaymentType == 2)
            paymentType = "Cash";
        else if (parsedPaymentType == 3)
            paymentType = "No charge";
        else if (parsedPaymentType == 4)
            paymentType = "Dispute";
        else if (parsedPaymentType == 5)
            paymentType = "Unknown";
        else if (parsedPaymentType == 6)
            paymentType = "Voided trip";
        return paymentType;
    }
}
