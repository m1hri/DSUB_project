package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Events {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "vehicle-rental-stream-v2"
        );
        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.111.1:9092"
        );
        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );
        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        // Send updated results every second instead of every 30 seconds
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> events =
                builder.stream("group4__business-events");

        KStream<String, String> rentals = events
                .filter((key, value) ->
                        value.contains("\"event_type\": \"RENTAL\"")
                )
                .selectKey((key, value) -> extractRentalId(value));

        KStream<String, String> payments = events
                .filter((key, value) ->
                        value.contains("\"event_type\": \"PAYMENT\"")
                )
                .selectKey((key, value) -> extractRentalId(value));

        KStream<String, String> joinedRentals = rentals.join(
                payments,
                (rental, payment) -> extractStartStation(rental) + "," + extractAmount(payment),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30))
        );

        KStream<String, String> revenueByStation = joinedRentals
                .selectKey((rentalId, result) -> result.split(",")[0]);
        revenueByStation
        .groupByKey()
        .aggregate(
                () -> "0,0.0",
                (station, value, stats) -> {
                    String[] parts = stats.split(",");

                    int count = Integer.parseInt(parts[0]) + 1;
                    double total = Double.parseDouble(parts[1]);

                    double amount =
                            Double.parseDouble(value.split(",")[1]);

                    double newTotal = total + amount;

                    return count + "," + newTotal;
                }
        )
        .toStream()
        .foreach((station, stats) -> {
            String[] parts = stats.split(",");

            int count = Integer.parseInt(parts[0]);
            double total = Double.parseDouble(parts[1]);

            REVENUE.put(station, new double[]{count, total});
        });

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

                    VEHICLES.put(key.key(), new double[]{count, (double) totalDuration / count});
                });


        KafkaStreams streams =
                new KafkaStreams(builder.build(), props);

        // Show errors in the console (otherwise the stream can stop silently)
        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("Stream error: " + e);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close)
        );
        streams.start();

        // Print the tables every 10 seconds
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(Events::printTables, 10, 10, TimeUnit.SECONDS);
    }

    private static final Map<String, double[]> REVENUE = new ConcurrentSkipListMap<>();
    private static final Map<String, double[]> VEHICLES = new ConcurrentSkipListMap<>();

    private static void printTables() {
        System.out.println();
        System.out.println("+----------------+---------+-------------+");
        System.out.println("| Station        | Rentals | Revenue CHF |");
        System.out.println("+----------------+---------+-------------+");

        if (REVENUE.isEmpty()) {
        System.out.println("| waiting for data ...                  |");
        }

        REVENUE.forEach((station, stats) ->
        System.out.printf(
                "| %-14s | %7d | %11.2f |%n",
                station,
                (int) stats[0],
                stats[1]
        )
        );
        System.out.println("+----------------+---------+-------------+");

        System.out.println();
        System.out.println("+----------------+---------+--------------+");
        System.out.println("| Vehicle type   | Rentals | Avg duration |");
        System.out.println("+----------------+---------+--------------+");
        if (VEHICLES.isEmpty()) System.out.println("| waiting for data ...                    |");
        VEHICLES.forEach((type, v) ->
                System.out.printf("| %-14s | %7d | %8.1f min |%n", type, (int) v[0], v[1]));
        System.out.println("+----------------+---------+--------------+");
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
    private static String extractRentalId(String json) {
        String search = "\"rental_id\": ";
        int start = json.indexOf(search) + search.length();
        int end = json.indexOf(",", start);

        return json.substring(start, end).trim();
    }
    private static String extractStartStation(String json) {
        String search = "\"start_station\": \"";
        int start = json.indexOf(search) + search.length();
        int end = json.indexOf("\"", start);

        return json.substring(start, end);
    }
    private static double extractAmount(String json) {
        String search = "\"amount\": ";
        int start = json.indexOf(search) + search.length();
        int end = json.indexOf("}", start);

        String amount = json.substring(start, end).trim();

        return Double.parseDouble(amount);
    }

}