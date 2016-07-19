/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.processing;

import com.datis.irc.entity.User;
import com.datis.irc.entity.UserMessages;
import com.datis.irc.pojo.JsonPOJODeserializer;
import com.datis.irc.pojo.JsonPOJOSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 *
 * @author jeus
 */
public class CountByKey {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "CountWord");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.17.0.11:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        
         Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<UserMessages> userMessageSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", UserMessages.class);
        userMessageSerializer.configure(serdeProps, false);// [T|F] hich farghi nadare :|

        final Deserializer<UserMessages> userMessageDesrializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", UserMessages.class);
        userMessageDesrializer.configure(serdeProps, true);// [T|F] hich farghi nadare :|

        final Serde<UserMessages> userSerde = Serdes.serdeFrom(userMessageSerializer, userMessageDesrializer);
//        final Serde<String> userSerde1 = Serdes.serdeFrom(Serdes.String(), Serdes.String());
         final Serde<String> userSerde1 = Serdes.String();
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, UserMessages> source = builder.stream(Serdes.String(),userSerde,"irc-messages-log");

        KTable<String, Long> counts = source.flatMapValues((UserMessages value) -> Arrays.asList(value.message.toLowerCase(Locale.getDefault()).split(" ")))
                .map((String key, String value) -> new KeyValue<>(value, value)).countByKey("Counts");

        // need to override value serde to Long type
        counts.print();
        counts.to(Serdes.String(), Serdes.Long(), "cw1");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();
    }
}
