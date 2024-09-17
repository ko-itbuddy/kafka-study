package org.ibtuddy.producers;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerAsyncWithCustomCB {

    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithCustomCB.class);

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        //1. Producer 환경 설정
        Properties props = new Properties();
        //bootstrap.servers, broker 주소
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key.serializer.class
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            IntegerSerializer.class.getName());
        // value.serializer.class
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        //2. 1에서 설정한 환경 설섲ㅇ값을 반영하여 KafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for(int seq=0; seq<20; seq++) {

            //3. 토픽명과 메시지 값(ket,value)을 입력하여 보낼 메시지인 ProducerRecord 객체 생성
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq,
                "hello-world " + seq);
            //4. KafkaProducer 객체의 send 메소드를 호출하여 ProducerRecord 전송

            CustomCallback callback = new CustomCallback(seq);
            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        kafkaProducer.close();

    }
}
