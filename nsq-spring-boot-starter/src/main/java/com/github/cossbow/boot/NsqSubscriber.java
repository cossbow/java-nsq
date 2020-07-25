package com.github.cossbow.boot;

import com.github.cossbow.nsq.util.ThrowoutFunction;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntToLongFunction;

public interface NsqSubscriber {

    /**
     * 默认解码器
     */
    Decoder defaultDecoder = NsqUtil.getDefaultDecoder();

    default <T> ThrowoutFunction<InputStream, T, IOException> newDecoder(Decoder decoder, Class<T> type) {
        return is -> decoder.decode(is, type);
    }

    //
    // 同步消费方法
    //

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer) {
        subscribe(topic, channel, type, defaultDecoder, consumer, 1);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer) {
        subscribe(topic, channel, type, decoder, consumer, 1);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, ThrowoutFunction<InputStream, T, IOException> decoder, Consumer<T> consumer) {
        subscribe(topic, channel, decoder, consumer, 1);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer, int concurrency) {
        subscribe(topic, channel, type, defaultDecoder, consumer, concurrency);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency) {
        subscribe(topic, channel, newDecoder(decoder, type), consumer, concurrency);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    <T> void subscribe(String topic, String channel, ThrowoutFunction<InputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency);

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer, int concurrency, int attemptsLimit) {
        subscribe(topic, channel, type, defaultDecoder, consumer, concurrency, attemptsLimit);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency, int attemptsLimit);

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay) {
        subscribe(topic, channel, type, defaultDecoder, consumer, concurrency, attemptDelay);
    }

    /**
     * 同步消费
     *
     * @param topic        话题名称
     * @param channel      消息订阅通道
     * @param type         消息类型
     * @param decoder      消息解码器，{@link Decoder}{@link Serializer}
     * @param consumer     消费回调
     * @param concurrency  并发数
     * @param attemptDelay 获取尝试延迟的回调。返回>0则延迟尝试，单位毫秒；0立即尝试，小于0则结束不再尝试
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay) {
        subscribe(topic, channel, newDecoder(decoder, type), consumer, concurrency, attemptDelay);
    }

    <T> void subscribe(String topic, String channel, ThrowoutFunction<InputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay);


    //
    // 异步消费方法
    //

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process) {
        subscribeAsync(topic, channel, type, process, 1);
    }

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency);

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency, int attemptsLimit);

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, IntToLongFunction attemptDelay) {
        subscribeAsync(topic, channel, type, process, 1, attemptDelay);
    }

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency, IntToLongFunction attemptDelay) {
        subscribeAsync(topic, channel, type, defaultDecoder, process, concurrency, attemptDelay);
    }

    /**
     * 异步消费
     *
     * @param topic        话题名称
     * @param channel      消息订阅通道
     * @param type         消息类型
     * @param process      消费函数，返回一个{@link CompletableFuture}
     * @param attemptDelay 获取尝试延迟的回调。返回>0则延迟尝试，单位毫秒；0立即尝试，小于0则结束不再尝试
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Decoder decoder, Function<T, CompletableFuture<?>> process, int concurrency, IntToLongFunction attemptDelay) {
        subscribeAsync(topic, channel, newDecoder(decoder, type), process, concurrency, attemptDelay);
    }

    <T> void subscribeAsync(String topic, String channel, ThrowoutFunction<InputStream, T, IOException> decoder, Function<T, CompletableFuture<?>> process, int concurrency, IntToLongFunction attemptDelay);


    //
    //
    //

    /**
     * 结束订阅话题，将关闭所有打开的订阅
     *
     * @param topic   话题名称
     * @param channel 消息订阅通道
     * @deprecated 这个方法未完成，有的时候不能正常工作，暂时不能使用
     */
    void unsubscribe(String topic, String channel);


}
