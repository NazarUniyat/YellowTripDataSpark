package com.course.plainJava;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.course.plainJava.FieldMapper.*;

public class AverageTipCalculation {
    public static void main(String[] args) {

        String path = args[0];

        Map<Trip, Double> tripDoubleMap = processInputFile(path);
        tripDoubleMap.entrySet().forEach(System.out::println);

    }


    private static Map<Trip, Double> processInputFile(String inputFilePath) {
        return readAllFilesIntoList(inputFilePath)
                .stream()
                .filter(tripTip -> !tripTip.getTrip().getPaymentType().equals("Unknown"))
                .collect(Collectors.groupingBy(TripTip::getTrip, Collectors.averagingDouble(TripTip::getTip)));
    }


    private static Function<String, TripTip> mapToItem = (line) -> {
        String[] p = line.split(",");
        Trip trip = new Trip();
        trip.setPickUpDateTime(parseDateTime(p[1]));
        trip.setTripDistance(parseDistance(p[4]));
        trip.setPaymentType(parsePayment(p[9]));
        TripTip tripTip = new TripTip();
        tripTip.setTrip(trip);
        tripTip.setTip(Double.parseDouble(p[13]));
        return tripTip;
    };


    private static List<TripTip> readAllFilesIntoList(String path) {

        ArrayList<TripTip> tripTipItems = new ArrayList<>();

        try (Stream<Path> walk = Files.walk(Paths.get(path)).skip(1)) {
            walk.forEach(path1 -> {
                File inputF = new File(String.valueOf(path1));
                InputStream inputFS = null;
                try {
                    inputFS = new FileInputStream(inputF);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
                tripTipItems.addAll(br.lines().skip(2)
                        .filter(line -> !line.split(",")[9].isEmpty())
                        .map(mapToItem)
                        .collect(Collectors.toList()));
            });
        } catch (IOException e) {
            System.out.println("lel");
        }

        return tripTipItems;
    }

}
