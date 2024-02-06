package com.appsdeveloperblog.emailnotification.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.appsdeveloperblog.emailnotification.error.NonRetryableException;
import com.appsdeveloperblog.emailnotification.error.RetryableException;
import com.appsdeveloperblog.ws.core.ProductCreatedEvent;

@Component
@KafkaListener(topics = "product-created-events")
public class ProductCreatedEventHandler {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private RestTemplate restTemplate;

  public ProductCreatedEventHandler(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @KafkaHandler
  public void handler(ProductCreatedEvent productCreateEvent) {
    logger.info("received a new event: " + productCreateEvent.getTitle());

    try {
      String reqURL = "http://localhost:8082/response/200";
      ResponseEntity<String> resp = restTemplate.exchange(reqURL, HttpMethod.GET, null, String.class);

      if (resp.getStatusCode().equals(HttpStatus.OK)) {
        logger.info(resp.getBody());
      }

    } catch (ResourceAccessException e) {
      logger.error(e.getMessage());
      throw new RetryableException(e);
    } catch (HttpServerErrorException e) {
      logger.error(e.getMessage());
      throw new NonRetryableException(e);
    } catch (Exception e) {
      logger.error(e.getMessage());
      throw new NonRetryableException(e);
    }

  }
}
