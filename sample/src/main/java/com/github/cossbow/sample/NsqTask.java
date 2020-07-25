package com.github.cossbow.sample;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.github.cossbow.boot.*;
import com.github.cossbow.boot.*;
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
    private static final Serializer jdkSerializer = new JdkSerializer();

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

    private static final String TOPIC_1 = "test-topic-1";
    private static final String TOPIC_2 = "test-topic-2";
    private static final String TOPIC_3 = "test-topic-3";
    private static final String TOPIC_3_RETURN = "test-topic-3-return";

    @Autowired
    private NsqSubscriber subscriber;
    @Autowired
    private NsqPublisher publisher;

    @Autowired
    @Qualifier(NsqUtil.QUALIFIER_PUB_SNAPPY)
    private NsqPublisher snappyPublisher;
    @Autowired
    @Qualifier(NsqUtil.QUALIFIER_PUB_DEFLATE)
    private NsqPublisher deflatePublisher;


    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);

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


    public void send1() {
        var rand = ThreadLocalRandom.current();
        var rate = new Rate();
        final int times = rand.nextInt(10, 20);
        for (int i = 1; i <= times; i++) {
            rate.setId(i);
            rate.setRoomTypeId(rand.nextLong(Integer.MAX_VALUE));
            rate.setRatePlanId(rand.nextLong(Integer.MAX_VALUE));
            publisher.publish(TOPIC_1, rate, jdkSerializer).exceptionally(e -> {
                log.error("publish fail", e);
                return null;
            });

        }
    }

    public void send2() {
        var rand = ThreadLocalRandom.current();
        final int times = rand.nextInt(10, 20);
        for (int i = 0; i < times; i++) {
            publisher.publish(TOPIC_2, ("this a message of ID=" + i).getBytes());
        }
    }

    public void send3() {
        var rand = ThreadLocalRandom.current();
        var rate = new Rate();
        final int times = rand.nextInt(1, 20);
        for (int i = 1; i <= times; i++) {
            rate.setId(i);
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
            var n = rand.nextInt(0, 99) % 1;
            switch (n) {
                case 0:
                    publisher.publish(TOPIC_3, rate, jsonSerializer);
                case 1:
                    snappyPublisher.publish(TOPIC_3, rate, jsonSerializer);
                    break;
                case 2:
                    deflatePublisher.publish(TOPIC_3, rate, jsonSerializer);
                    break;
            }
        }
    }


    private void startAsyncTopic1() {
        subscriber.subscribeAsync(TOPIC_1, "default", Rate.class, jdkSerializer, rate -> {
            log.debug("async rate={}", rate);
            return null;
        }, concurrency, NsqUtil.attemptLimit(1000, 10));
    }

    private void startTopic1() {
        subscriber.subscribe(TOPIC_1, "default", Rate.class, jdkSerializer, rate -> {
            log.debug("sync rate={}", rate);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, concurrency);
    }

    private void startStreamTopic2() {
        subscriber.subscribe(TOPIC_2, "default", NsqUtil.STRING_DECODER, s -> {
            log.debug("bytes received: {}", s);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, concurrency);
    }

    private void startTopic3() {
        subscriber.subscribe(TOPIC_3, "default", Rate.class, jsonSerializer, rate -> {
            log.debug("TOPIC_3 rate={}", rate);
        }, concurrency);
    }

    private void startTopic3Return() {
        subscriber.subscribe(TOPIC_3_RETURN, "java", Rate.class, jsonSerializer, rate -> {
            log.debug("TOPIC_3_RETURN rate={}", rate);
        }, concurrency);
    }


    private void type1() {
        startTopic1();
        executorService.scheduleAtFixedRate(this::send1, 0, sendRate, TimeUnit.MILLISECONDS);
    }

    private void type2() {
        startAsyncTopic1();
        executorService.scheduleAtFixedRate(this::send1, 0, sendRate, TimeUnit.MILLISECONDS);
    }

    private void type3() {
        startStreamTopic2();
        executorService.scheduleAtFixedRate(this::send2, 0, sendRate, TimeUnit.MILLISECONDS);
    }

    private void type4() {
        startTopic3();
        startTopic3Return();
        executorService.scheduleAtFixedRate(this::send3, 0, sendRate, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        switch (type) {
            case 1:
                type1();
                break;
            case 2:
                type2();
                break;
            case 3:
                type3();
                break;
            case 4:
                type4();
                break;
            default:
                throw new IllegalArgumentException("invalid type value: " + type);
        }

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
