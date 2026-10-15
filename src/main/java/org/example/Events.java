package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.JoinWindows;
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

joinedRentals.foreach((rentalId, result) -> {
    System.out.println("JOIN für rental_id: " + rentalId);
    System.out.println(result);
});
KStream<String, String> revenueByStation = joinedRentals
        .selectKey((rentalId, result) -> result.split(",")[0]);
revenueByStation
        .groupByKey()
        .aggregate(
                () -> "0.0",
                (station, value, total) -> {
                    double amount = Double.parseDouble(value.split(",")[1]);
                    double newTotal = Double.parseDouble(total) + amount;

                                        return String.valueOf(newTotal);
                }
        )
        .toStream()
        .foreach((station, total) -> {
            System.out.println(
                    "Startstation: " + station
                    + " | Gesamtumsatz: " + String.format("%.2f", Double.parseDouble(total)) + " CHF"
            );
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