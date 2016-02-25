package com.mapr.streams.demo;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import rx.Observable;
import rx.apache.http.ObservableHttp;
import rx.apache.http.ObservableHttpResponse;

import java.util.Properties;

/**
 * Created by mlalapet on 2/24/16.
 */
public class CommentEventsProducer {

    //api key 767a633f5a382a5f4247507d71126d32
    public static String topic = "/demostreams/meetups:comments";
    public static KafkaProducer producer;

    public static void main(String args[]) {

        configureProducer(args);

        CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
        httpClient.start();
        Observable<ObservableHttpResponse> response = ObservableHttp.createRequest(HttpAsyncMethods.createGet("https://stream.meetup.com/2/event_comments"), httpClient)
                .toObservable();
        Observable<byte[]> bytes = response.flatMap(ObservableHttpResponse::getContent);
        Observable<String> chunks = bytes.map(content -> new String(content));

        chunks.toBlocking().forEach(chunk -> {
            String results[] = chunk.split("\n");
            for(String result : results) {
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, result);
                producer.send(rec);
                System.out.println("Sent message");
            }
        });

        //TODO how to gracefully close them and have only one instance of producer for kafka
        //producer.close();
        //httpClient.close();
    }

    public static void configureProducer(String[] args) {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

}
