package com.layo.kafkaexample.engine;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.annotation.KafkaListener;




@Service
@ConditionalOnProperty(value = "example.kafka.consumer-enabled", havingValue = "true")
public class Consumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = {"INPUT_DATA"})
    public void consume(
            final @Payload String message,
            final @Header(KafkaHeaders.OFFSET) int offset,
            final @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            final @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
            final  Acknowledgment acknowledgment) {
        logger.info(String.format("#### -> Consume message -> TIMESTAMP: %d\n%s\noffset: %d\nkey: %s\npartition: %d\ntopic: %s", ts, message, offset,key,partition,topic));
        acknowledgment.acknowledge();
    }
}
