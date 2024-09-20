package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.model.Player;
import org.example.model.Product;
import org.example.model.ScoreEvent;
import org.example.model.join.Enriched;
import org.example.model.join.ScoreWithPlayer;
import org.example.serialization.json.JsonSerdes;

public class LeaderBoardTopology {
    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ScoreEvent> scoreEvents =
                builder.stream(
                        "score-events",
                        Consumed.with(Serdes.ByteArray(), JsonSerdes.ScoreEvent()))
                        .selectKey((k, v) -> v.getPlayerId().toString());

        KTable<String, Player> players = builder.table("players", Consumed.with(Serdes.String(), JsonSerdes.Player()));

        GlobalKTable<String, Product> products = builder.globalTable("products", Consumed.with(Serdes.String(), JsonSerdes.Product()));

        Joined<String, ScoreEvent, Player> playerJoinParams =
                Joined.with(Serdes.String(), JsonSerdes.ScoreEvent(), JsonSerdes.Player());

        ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> scorePlayerJoiner = (score, player) -> new ScoreWithPlayer(score, player);

        KStream<String, ScoreWithPlayer> withPlayers =
                scoreEvents.join(players, scorePlayerJoiner, playerJoinParams);

        KeyValueMapper<String, ScoreWithPlayer, String> keyMapper =
                (leftKey, scoreWithPlayer) -> {
                    return String.valueOf(scoreWithPlayer.getScoreEvent().getProductId());
                };

        ValueJoiner<ScoreWithPlayer, Product, Enriched> productJoiner =
                (scoreWithPlayer, product) -> new Enriched(scoreWithPlayer, product);
        KStream<String, Enriched> withProducts = withPlayers.join(products, keyMapper, productJoiner);
        withProducts.print(Printed.<String, Enriched>toSysOut().withLabel("with-products"));

        KGroupedStream<String, Enriched> grouped =
                withProducts.groupBy(
                        (key, value) -> value.getProductId().toString(),
                        Grouped.with(Serdes.String(), JsonSerdes.Enriched()));

        Initializer<HighScores> highScoresInitializer = HighScores::new;

        Aggregator<String, Enriched, HighScores> highScoresAdder =
                (key, value, aggregate) -> aggregate.add(value);

        KTable<String, HighScores> highScores =
                grouped.aggregate(
                        highScoresInitializer,
                        highScoresAdder,
                        Materialized.<String, HighScores, KeyValueStore<Bytes, byte[]>>
                                // give the state store an explicit name to make it available for interactive
                                // queries
                                        as("leader-boards")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.HighScores()));

        highScores.toStream().to("high-scores");
        return builder.build();
    }
}
