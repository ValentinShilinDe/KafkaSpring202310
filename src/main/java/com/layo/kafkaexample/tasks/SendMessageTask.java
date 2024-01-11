package com.layo.kafkaexample.tasks;
import com.layo.kafkaexample.engine.Producer;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.LocalDate;
import java.util.concurrent.ExecutionException;


@Component
public class SendMessageTask {
    private  final Logger logger = LoggerFactory.getLogger(SendMessageTask.class);

    private final Producer producer;
    public SendMessageTask(Producer producer)
    {
        this.producer = producer;
    }

    @Scheduled(fixedRateString = "1000")
    public void send() throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<String, String>> listenableFuture = this.producer.sendMessage("INPUT_DATA", "KEY", LocalDate.now().toString());

        SendResult<String, String> result = listenableFuture.get();
        logger.info(String.format("Producer:\ntopic: %s\noffset:%d\n partition: %d\nvalue size: %d",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().offset(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().serializedValueSize()));
    }

}
