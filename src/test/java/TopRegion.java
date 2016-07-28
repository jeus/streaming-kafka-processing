/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author jeus
 */
import io.confluent.examples.streams.utils.GenericAvroSerde;
import io.confluent.examples.streams.utils.PriorityQueueSerde;
import io.confluent.examples.streams.utils.WindowedSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.File;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;

/**
 * Create a data feed of the top 100 news articles per industry, ranked by
 * click-through-rate (assuming this is for the past hour).
 *
 * Note: The generic Avro binding is used for serialization/deserialization.
 * This means the appropriate Avro schema files must be provided for each of the
 * "intermediate" Avro classes, i.e. whenever new types of Avro objects (in the
 * form of GenericRecord) are being passed between processing steps.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class TopRegion {

    public static boolean isArticle(GenericRecord record) {
        String flags = (String) record.get("flags");

        return flags.contains("ART");
    }

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "top-articles-lambda-example");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.17.0.11:2181");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<GenericRecord> avroSerde = new GenericAvroSerde();
        final Serde<Windowed<String>> windowedStringSerde = new WindowedSerde<>(stringSerde);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<byte[], GenericRecord> views = builder.stream("PageViews");

        KStream<GenericRecord, GenericRecord> articleViews = views
                // filter only article pages
                .filter((dummy, record) -> isArticle(record))
                .map((dummy, article) -> new KeyValue<>(article, article));

        Schema schema = new Schema.Parser().parse(new File("pageviewstats.avsc"));

        KTable<Windowed<GenericRecord>, Long> viewCounts = articleViews
                // count the clicks per hour, using tumbling windows with a size of one hour
                //                .groupByKey(avroSerde, avroSerde)
                .countByKey(TimeWindows.of("PageViewCountWindows", 60 * 60 * 1000L), avroSerde);

        KTable<Windowed<String>, PriorityQueue<GenericRecord>> allViewCounts = viewCounts
                .groupBy(
                        // the selector
                        (windowedArticle, count) -> {
                            // project on the industry field for key
                            Windowed<String> windowedIndustry
                            = new Windowed<>((String) windowedArticle.key().get("industry"), windowedArticle.window());
                            // add the page into the value
                            GenericRecord viewStats = new GenericData.Record(schema);
                            viewStats.put("page", "pageId");
                            viewStats.put("industry", "industryName");
                            viewStats.put("count", count);
                            return new KeyValue<>(windowedIndustry, viewStats);
                        },
                        windowedStringSerde,
                        avroSerde
                ).aggregate(
                        // the initializer
                        () -> {
                            Comparator<GenericRecord> comparator
                            = (o1, o2) -> (int) ((Long) o1.get("count") - (Long) o2.get("count"));
                            return new PriorityQueue<>(comparator);
                        },
                        // the "add" aggregator
                        (windowedIndustry, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },
                        // the "remove" aggregator
                        (windowedIndustry, record, queue) -> {
                            queue.remove(record);
                            return queue;
                        },
                        new PriorityQueueSerde<>(),
                        "AllArticles"
                );

        int topN = 100;
        KTable<Windowed<String>, String> topViewCounts = allViewCounts
                .mapValues(queue -> {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < topN; i++) {
                        GenericRecord record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        sb.append((String) record.get("page"));
                        sb.append("\n");
                    }
                    return sb.toString();
                });

        topViewCounts.to(windowedStringSerde, stringSerde, "TopNewsPerIndustry");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }

}
