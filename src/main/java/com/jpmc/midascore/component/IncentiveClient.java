package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/** Asks the incentive API how much bonus a validated transaction earns. Fails safe to a zero incentive. */
@Component
public class IncentiveClient {
    private static final Logger logger = LoggerFactory.getLogger(IncentiveClient.class);

    private final RestTemplate restTemplate;
    private final String url;

    public IncentiveClient(RestTemplateBuilder builder, @Value("${general.incentive-api-url}") String url) {
        this.restTemplate = builder
                .setConnectTimeout(Duration.ofSeconds(2))
                .setReadTimeout(Duration.ofSeconds(2))
                .build();
        this.url = url;
    }

    public float incentiveFor(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(url, transaction, Incentive.class);
            return incentive == null ? 0f : incentive.getAmount();
        } catch (RestClientException e) {
            logger.warn("incentive API unavailable ({}); applying no incentive to {}", e.getMessage(), transaction);
            return 0f;
        }
    }
}
