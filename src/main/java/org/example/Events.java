package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

public class Events {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "vehicle-rental-stream"
        );
        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );
        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );
        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> events =
                builder.stream("business-events");

        events
                .filter((key, value) ->
                        value.contains("\"event_type\": \"RENTAL_RETURNED\"")
                )
                .selectKey((key, value) -> extractVehicleType(value))

                .groupByKey()
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                Duration.ofSeconds(10),
                                Duration.ofSeconds(1)
                        )
                )


                .aggregate(
                        () -> "0,0",
                        (vehicleType, value, stats) -> {
                            String[] parts = stats.split(",");
                            int count = Integer.parseInt(parts[0]) + 1;
                            int totalDuration = Integer.parseInt(parts[1])
                                    + extractDuration(value);

                            return count + "," + totalDuration;
                        }
                )
                .toStream()
                .foreach((key, stats) -> {
                    String[] parts = stats.split(",");
                    int count = Integer.parseInt(parts[0]);
                    int totalDuration = Integer.parseInt(parts[1]);

                    System.out.println(
                            "Vehicle type: " + key.key()
                                    + " | Rentals: " + count
                                    + " | Average duration: "
                                    + String.format(
                                    "%.2f",
                                    (double) totalDuration / count
                            )
                                    + " minutes"
                    );
                });


        KafkaStreams streams =
                new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close)
        );
        streams.start();
    }

    private static String extractVehicleType(String json) {
        String search = "\"vehicle_type\": \"";
        int start = json.indexOf(search) + search.length();
        int end = json.indexOf("\"", start);
        return json.substring(start, end);
    }


    private static int extractDuration(String json) {
        String search = "\"rental_duration_minutes\": ";
        int start = json.indexOf(search) + search.length();
        int end = json.indexOf("\n", start);

        String duration = json.substring(start, end)
                .replace(",", "")
                .trim();
        return Integer.parseInt(duration);
    }

}