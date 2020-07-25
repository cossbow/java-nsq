package com.github.cossbow.nsq.util;

@FunctionalInterface
public interface ThrowoutFunction<T, R, E extends Throwable> {

    R apply(T t) throws E;


    //

    static <T, E extends Throwable> ThrowoutFunction<T, T, E> identity() {
        return t -> t;
    }

}
