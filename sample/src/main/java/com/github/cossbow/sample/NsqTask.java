package com.github.cossbow.sample;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.github.cossbow.boot.NsqProperties;
import com.github.cossbow.pubsub.NsqPublisher;
import com.github.cossbow.pubsub.NsqSubscriber;
import com.github.cossbow.pubsub.Serializer;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@ConfigurationProperties(prefix = "test")
public class NsqTask implements ApplicationRunner {


    private static final Serializer jsonSerializer;

    static {
        ObjectMapper mapper = new ObjectMapper();
        var module = new SimpleModule();
        module.addSerializer(Instant.class, new JsonSerializer<>() {
            @Override
            public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                if (null != value) gen.writeString(value.toString());
            }
        });
        module.addDeserializer(Instant.class, new JsonDeserializer<>() {
            @Override
            public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                var s = p.getValueAsString();
                return null == s ? null : Instant.parse(s);
            }
        });
        mapper.registerModule(module);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.enable(SerializationFeature.WRITE_ENUMS_USING_INDEX);
        mapper.configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonSerializer = new JacksonSerializer(mapper);

    }

    //

    private static final String TOPIC = "test-topic-jjj";

    //

    @Autowired
    private NsqSubscriber subscriber;
    @Autowired
    private NsqPublisher publisher;

    @Autowired
    @Qualifier(NsqProperties.QUALIFIER_PUB_SNAPPY)
    private NsqPublisher snappyPublisher;
    @Autowired
    @Qualifier(NsqProperties.QUALIFIER_PUB_DEFLATE)
    private NsqPublisher deflatePublisher;


    private final ScheduledExecutorService executorService =
            Executors.newScheduledThreadPool(16);

    @Setter
    @Getter
    private int type;
    @Setter
    @Getter
    private long sendRate = 1000;
    @Setter
    @Getter
    private int concurrency = 1;
    @Setter
    @Getter
    private int batchInterval = 1000;


    private Rate newRate(int id) {
        var rand = ThreadLocalRandom.current();
        var rate = new Rate();
        rate.setId(id);
        rate.setRoomTypeId(rand.nextLong(Integer.MAX_VALUE));
        rate.setRatePlanId(rand.nextLong(Integer.MAX_VALUE));
        var dailies = rand.ints().limit(rand.nextInt(8, 29)).mapToObj(j -> {
            var daily = new DailyConsumerPrice();
            daily.setRateId(rate.getId());
            daily.setSelling(rand.nextBoolean());
            daily.setAmount(rand.nextInt(10000, 100000));
            daily.setDate(ValueUtil.fromDate(LocalDate.of(2020,
                    rand.nextInt(1, 13),
                    rand.nextInt(1, 29))));
            daily.setCreatedAt(Instant.now());
            daily.setUpdatedAt(daily.getCreatedAt().plusSeconds(rand.nextLong(999, 99999)));
            return daily;
        }).collect(Collectors.toList());
        rate.setDailyConsumerPrices(dailies);
        return rate;
    }

    private NsqPublisher randPublisher() {
        var rand = ThreadLocalRandom.current();
        var n = rand.nextInt(0, 99) % 3;
        switch (n) {
            case 0:
                return publisher;
            case 1:
                return snappyPublisher;
            case 2:
            default:
                return deflatePublisher;
        }
    }

    public void sendJson() {
        var rand = ThreadLocalRandom.current();
        final int times = rand.nextInt(1, 20);
        for (int i = 1; i <= times; i++) {
            var rate = newRate(i);
            var publisher = randPublisher();
            if (rand.nextBoolean()) {
                publisher.publish(TOPIC, rate, jsonSerializer);
            } else {
                publisher.publish(TOPIC, 1000, rate, jsonSerializer);
            }
        }
    }


    private void startTopic() {
        subscriber.subscribe(TOPIC, "java", Rate.class, jsonSerializer, rate -> {
            log.debug("get rate={}", rate);
        }, concurrency);
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        startTopic();
        executorService.scheduleAtFixedRate(this::sendJson, 0, sendRate, TimeUnit.MILLISECONDS);
    }


    //


    @Data
    static class DailyConsumerPrice implements Serializable {
        private static final long serialVersionUID = -6668967514743726329L;

        private long rateId;
        private Instant date;

        private int amount;
        private Instant createdAt;
        private Instant updatedAt;
        private String channel;
        private boolean selling;

    }

    @Data
    static class Rate implements Serializable {
        private static final long serialVersionUID = 914759460378768L;

        private long id;
        private long roomTypeId;
        private long ratePlanId;
        private long hotelId;
        private boolean enable;

        private List<DailyConsumerPrice> dailyConsumerPrices;

    }

}
