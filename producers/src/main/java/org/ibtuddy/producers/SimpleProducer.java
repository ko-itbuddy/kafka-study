package org.ibtuddy.producers;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

    public static void main(String[] args) {

        String topicName = "simple-topic";

        //1. Producer 환경 설정
        Properties props = new Properties();
        //bootstrap.servers, broker 주소
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key.serializer.class
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        // value.serializer.class
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        //2. 1에서 설정한 환경 설섲ㅇ값을 반영하여 KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //3. 토픽명과 메시지 값(ket,value)을 입력하여 보낼 메시지인 ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
            "hello-world");
        //4. KafkaProducer 객체의 send 메소드를 호출하여 ProducerRecord 전송
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();

        // 5.KafkaProducer 객체의 close() 메소드를 호출하여 종료

        kafkaProducer.close();
    }
}
