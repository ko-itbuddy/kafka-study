package org.ibtuddy.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerAsync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class);

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
//        kafkaProducer.send(producerRecord, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                if(e == null){
//                    logger.info("\n ##### record metadata received  #####\n" +
//                        "partition:"+ recordMetadata.partition() + "\n"+
//                        "offset:" + recordMetadata.offset() + "\n" +
//                        "timestamp:" + recordMetadata.timestamp()
//                    );
//                }else{
//                    logger.error("exception error form broker " + e.getMessage());
//                }
//
//            }
//        });

        kafkaProducer.send(producerRecord, (recordMetadata, exception)->{
            if(exception == null){
                    logger.info("\n ##### record metadata received  #####\n" +
                        "partition:"+ recordMetadata.partition() + "\n"+
                        "offset:" + recordMetadata.offset() + "\n" +
                        "timestamp:" + recordMetadata.timestamp()
                    );
                }else{
                    logger.error("exception error form broker " + exception.getMessage());
                }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        kafkaProducer.close();

    }
}
