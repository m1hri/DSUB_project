package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class EventGenerator {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        String topic = "business-events";

        Random random = new Random();

        while (true) {

            int eventType = random.nextInt(3);

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
                        1000 + random.nextInt(100),
                        1 + random.nextInt(10),
                        101 + random.nextInt(8),
                        getVehicleType(random),
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
                        1000 + random.nextInt(100),
                        101 + random.nextInt(8),
                        getVehicleType(random),
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
                        1000 + random.nextInt(100),
                        1 + random.nextInt(10),
                        getAgeGroup(random),
                        5 + random.nextDouble() * 30
                );
            }

            producer.send(
                    new ProducerRecord<>("business-events", event)
            );

            System.out.println("Sent event: " + event);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
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