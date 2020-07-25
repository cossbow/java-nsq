package com.github.cossbow.nsq;


import com.github.cossbow.nsq.lookup.DefaultNSQLookup;
import com.github.cossbow.nsq.lookup.NSQLookup;
import com.github.cossbow.nsq.util.ThrowoutFunction;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Nsq {

    public static String getNsqdHost() {
        String hostName = System.getenv("NSQD_HOST");
        if (hostName == null) {
            hostName = "localhost";
        }
        return hostName;
    }

    public static String getNsqLookupdHost() {
        String hostName = System.getenv("NSQLOOKUPD_HOST");
        if (hostName == null) {
            hostName = "localhost";
        }
        return hostName;
    }

    @Test
    public void lookup() {
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        lookup.lookupNodeAsync().thenAccept(serverAddresses -> {
            for (var sa : serverAddresses) {
                System.out.println(sa);
            }
        }).join();

    }


    public static final NSQConfig CONFIG = new NSQConfig();

    ThrowoutFunction<InputStream, String, IOException> STRING_DECODER = in -> {
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
