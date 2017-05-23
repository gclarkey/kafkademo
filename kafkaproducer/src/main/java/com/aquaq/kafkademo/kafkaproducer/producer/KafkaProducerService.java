package com.aquaq.kafkademo.kafkaproducer.producer;

import com.aquaq.kafkademo.api.CurrencyRateMessage;
import lombok.Synchronized;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private static Long messageId;
    private static Double price;
    @Value("${kafka.topic}")
    public String topic;
    private Random random;
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostConstruct
    public void postConstructInit() {
        final Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final ProducerFactory producerFactory = new DefaultKafkaProducerFactory<>(producerConfigs);

        kafkaTemplate = new KafkaTemplate<>(producerFactory);
        messageId = 0L;
        price = 1000.0;
        random = new Random();
    }

    @Scheduled(fixedRateString = "${kafka.message.publish-interval}")
    public void sendMessage() {
        final CurrencyRateMessage currencyRateMessage = createMessage();
        send(topic, currencyRateMessage.toString());
        updatePriceForNextSchedule();
    }

    @Synchronized
    private CurrencyRateMessage createMessage() {
        final CurrencyRateMessage currencyRateMessage = new CurrencyRateMessage();
        currencyRateMessage.setId(messageId++);
        currencyRateMessage.setFromCurrency("GBP");
        currencyRateMessage.setToCurrency("USD");
        currencyRateMessage.setTime(LocalTime.now());
        currencyRateMessage.setPrice1(price);
        currencyRateMessage.setPrice2(price);
        currencyRateMessage.setPrice3(price);
        currencyRateMessage.setPrice4(price);

        return currencyRateMessage;
    }

    private void send(String topic, String message) {
        // the KafkaTemplate provides asynchronous send methods returning a Future
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

        // register a callback with the listener to receive the result of the send asynchronously
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info("sent message='{}' with offset={}", message,
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error("unable to send message='{}'", message, ex);
            }
        });

        // or, to block the sending thread to await the result, invoke the future's get() method
    }

    private void updatePriceForNextSchedule() {
        final Double delta = 0.5 - random.nextDouble();
        price = price += delta;
    }
}
