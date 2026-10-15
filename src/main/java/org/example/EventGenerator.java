package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class EventGenerator {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.1:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        String topic = "group4__business-events";

        Random random = new Random();

        int nextRentalId = 1000;
        while (true) {

        int rentalId = nextRentalId++;
        int customerId = 1 + random.nextInt(10);
        int vehicleId = 101 + random.nextInt(8);
        String vehicleType = getVehicleType(random);

        for (int eventType = 0; eventType < 3; eventType++) {

         String event;

            if (eventType == 0) {
                event = """
                        {
                          "event_type": "RENTAL",
                          "rental_id": %d,
                          "customer_id": %d,
                          "vehicle_id": %d,
                          "vehicle_type": "%s",
                          "start_station": "%s"
                        }
                        """.formatted(
                        rentalId,
                        customerId,
                        vehicleId,
                        vehicleType,
                        getStation(random)
                );

            } else if (eventType == 1) {
                event = """
                        {
                          "event_type": "RENTAL_RETURNED",
                          "rental_id": %d,
                          "vehicle_id": %d,
                          "vehicle_type": "%s",
                          "end_station": "%s",
                          "rental_duration_minutes": %d
                        }
                        """.formatted(
                        rentalId,
                        vehicleId,
                        vehicleType,
                        getStation(random),
                        30 + random.nextInt(61)
                );

            } else {
                event = """
                        {
                          "event_type": "PAYMENT",
                          "rental_id": %d,
                          "customer_id": %d,
                          "customer_age_group": "%s",
                          "amount": %.2f
                        }
                        """.formatted(
                        rentalId,
                        customerId,
                        getAgeGroup(random),
                        5 + random.nextDouble() * 30
                );
            }

            try {
                producer.send(
                new ProducerRecord<>(topic, event)
                ).get();
                System.out.println("Kafka hat das Event bestätigt!");
                } catch (Exception e) {
                System.err.println("Kafka-Versand fehlgeschlagen: " + e.getMessage());
            break;
}

            System.out.println("Sent event: " + event);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    }

    private static String getVehicleType(Random random) {
        String[] types = {"E-Bike", "Electric Car", "Car", "Bike"};
        return types[random.nextInt(types.length)];
    }

    private static String getStation(Random random) {
        String[] stations = {
                "Hauptbahnhof",
                "Langstrasse",
                "Hardbrücke",
                "Stadelhofen",
                "Oerlikon"
        };
        return stations[random.nextInt(stations.length)];
    }

    private static String getAgeGroup(Random random) {
        String[] groups = {"Under 25", "25-34", "35-44", "45+"};
        return groups[random.nextInt(groups.length)];
    }
}