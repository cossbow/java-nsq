package com.github.cossbow.sample;

import com.github.cossbow.boot.NsqAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;


@Import(NsqAutoConfiguration.class)
@SpringBootApplication
public class NsqTestApp {

    public static void main(String[] args) {
        SpringApplication.run(NsqTestApp.class, args);
    }

}
