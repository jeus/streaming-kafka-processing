/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.region;

//import com.datis.irc.entity.UserMessages;
//import com.datis.irc.pojo.JsonPOJODeserializer;
//import com.datis.irc.pojo.JsonPOJOSerializer;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Locale;
//import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.common.serialization.Serializer;
//import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
//import org.apache.kafka.streams.kstream.ValueMapper;
//import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 *
 * @author jeus
 */
public class RegionCount {

    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    public static void main(String[] arg) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "regionStep1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.17.0.11:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,Serdes.Long().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<Long> longSerde = Serdes.Long();

        final Serde<String> strSerde = Serdes.String();
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Long, String> source = builder.stream(longSerde, strSerde, "step1");
        System.out.println("Source :"+source.toString());

        
        KTable<String, Long> counts = source.map((Long key, String value) -> new KeyValue<String , String>(value, value)).countByKey("count");
//.map((Long key, String value) -> new KeyValue<>(value, value)).countByKey("Counts");
        counts.print();
        counts.to(Serdes.String(), Serdes.Long(), "step2");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
//        Thread.sleep(5000L);
//
//        streams.close();
    }

}
