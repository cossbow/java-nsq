package com.github.cossbow.boot;

import com.github.cossbow.nsq.CompressType;
import com.github.cossbow.nsq.NSQConfig;
import com.github.cossbow.nsq.lookup.DefaultNSQLookup;
import com.github.cossbow.nsq.lookup.NSQLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.StandardEnvironment;
import org.xerial.snappy.Snappy;

import java.net.URI;
import java.util.Objects;


@Configuration
@EnableConfigurationProperties({NsqProperties.class})
public class NsqAutoConfiguration {
    private static final Logger log = LoggerFactory.getLogger(NsqAutoConfiguration.class);

    private static final String PACKAGE_NAME = "com.fmrt.trade.nsq";
    private static final String NSQ_PUBLISHER_BEAN_NAME = PACKAGE_NAME + ".nsqPublisher";
    private static final String NSQ_SUBSCRIBER_BEAN_NAME = PACKAGE_NAME + ".nsqSubscriber";


    private final NsqProperties properties;


    //
    //


    //
    //


    @Autowired
    public NsqAutoConfiguration(NsqProperties properties,
                                StandardEnvironment environment) {
        this.properties = properties;

        String lookupAddress = properties.getLookupAddress();
        log.info("NSQ lookupAddress: " + lookupAddress);

    }


    @Lazy
    @Bean
    @ConditionalOnMissingBean
    NSQLookup nsqLookup() {
        String lookupAddress = properties.getLookupAddress();
        var nsqLookup = new DefaultNSQLookup();
        var uri = URI.create("http://" + lookupAddress);
        nsqLookup.addLookupAddress(uri.getHost(), uri.getPort());
        return nsqLookup;
    }

    @Lazy
    @Bean(name = NSQ_PUBLISHER_BEAN_NAME)
    @Primary
    @ConditionalOnMissingBean
    NsqPublisher nsqPublisher(NSQLookup nsqLookup) {

        return new NsqPublisherWrapper(nsqLookup);
    }

    @Lazy
    @Bean
    @Qualifier(NsqUtil.QUALIFIER_PUB_SNAPPY)
    @ConditionalOnClass(Snappy.class)
    NsqPublisher snappyPublisher(NSQLookup nsqLookup) {
        var config = new NSQConfig();
        config.setCompress(CompressType.Snappy);
        return new NsqPublisherWrapper(nsqLookup, config);
    }

    @Lazy
    @Bean
    @Qualifier(NsqUtil.QUALIFIER_PUB_DEFLATE)
    NsqPublisher deflatePublisher(NSQLookup nsqLookup) {
        var config = new NSQConfig();
        config.setCompress(CompressType.Deflate);
        return new NsqPublisherWrapper(nsqLookup, config);
    }

    @Lazy
    @Bean(name = NSQ_SUBSCRIBER_BEAN_NAME)
    @ConditionalOnMissingBean
    NsqSubscriber nsqSubscriber(NSQLookup nsqLookup,
                                StandardEnvironment environment) {
        var consumerAgent = Objects.requireNonNullElse(
                properties.getUserAgent(),
                environment.getProperty(
                        "spring.application.name",
                        "Java"));
        return new NsqSubscriberWrapper(
                nsqLookup,
                properties.getLookupPeriodMillis(),
                properties.getDefaultAttemptLimit(),
                properties.getDefaultAttemptDelay(),
                properties.getSchedulerPoolSize(),
                properties.getDefaultBatchInterval(),
                consumerAgent);
    }

    //


}
