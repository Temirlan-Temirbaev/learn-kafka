package org.example;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        Topology topology = LeaderBoardTopology.build();

        String host = System.getProperty("host", "localhost");
        Integer port = Integer.parseInt(System.getProperty("port", "8080"));
        String stateDir = System.getProperty("stateDir", "/tmp/kafka-streams");
        String endpoint = String.format("%s:%s", host, port);

        // set the required properties for running Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        // build the topology
        System.out.println("Starting Videogame Leaderboard");
        KafkaStreams streams = new KafkaStreams(topology, props);
        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        // start streaming!
        streams.start();

        HostInfo hostInfo = new HostInfo(host, port);
        LeaderboardService service = new LeaderboardService(hostInfo, streams);
        service.start();
    }
}