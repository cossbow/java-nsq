package com.github.cossbow.pubsub;

import com.github.cossbow.nsq.util.ThrowoutFunction;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.IntToLongFunction;

final
public class NsqUtil {
    private NsqUtil() {
    }

    private final static Serializer defaultSerializer = new JdkSerializer();

    public static Encoder getDefaultEncoder() {
        return defaultSerializer;
    }

    public static Decoder getDefaultDecoder() {
        return defaultSerializer;
    }


    //
    //
    //

    final static int ATTEMPT_STOP_VALUE = -1;

    public static IntToLongFunction attemptNot() {
        return a -> ATTEMPT_STOP_VALUE;
    }

    public static IntToLongFunction attemptAlways(int delay) {
        return a -> delay;
    }

    public static IntToLongFunction attemptLimit(int delay, int limit) {
        return attemptLimit(a -> delay, limit);
    }

    public static IntToLongFunction attemptLimit(IntToLongFunction get, int limit) {
        return a -> {
            if (a < limit) {
                return get.applyAsLong(a);
            } else {
                return ATTEMPT_STOP_VALUE;
            }
        };
    }


    //
    //
    //


    public static final ThrowoutFunction<InputStream, String, IOException> STRING_DECODER = in -> {
        var reader = new InputStreamReader(in);
        var sb = new StringBuilder();
        var buf = new char[1024];
        int nRead;
        while ((nRead = reader.read(buf)) != -1) {
            sb.append(buf, 0, nRead);
        }
        return sb.toString();
    };

}
