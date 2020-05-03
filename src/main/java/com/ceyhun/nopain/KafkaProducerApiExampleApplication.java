package com.ceyhun.nopain;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.web.SpringDataWebAutoConfiguration;

/**
 * @author ceyhunuzunoglu
 */
@SpringBootApplication(scanBasePackages = "com.ceyhun.nopain", exclude = {SpringDataWebAutoConfiguration.class})
public class KafkaProducerApiExampleApplication {

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(KafkaProducerApiExampleApplication.class);
    app.setRegisterShutdownHook(false);
    app.run(args);
  }
}
