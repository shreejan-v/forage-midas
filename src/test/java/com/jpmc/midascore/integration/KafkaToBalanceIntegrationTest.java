package com.jpmc.midascore.integration;

import com.jpmc.midascore.FileLoader;
import com.jpmc.midascore.KafkaProducer;
import com.jpmc.midascore.UserPopulator;
import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.foundation.Balance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * End to end: Forage user and transaction files -> embedded Kafka -> listener -> H2 -> GET /balance.
 * The incentive API is mocked to return 0, so the expected balances below were computed independently
 * (a short Python reference script applying the same validation rules in float32) from the same files.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class KafkaToBalanceIntegrationTest {

    private static final Map<Long, Float> EXPECTED = Map.ofEntries(
            Map.entry(0L, 0f), Map.entry(1L, 1326.98f), Map.entry(2L, 2567.52f), Map.entry(3L, 2740.33f),
            Map.entry(4L, 140.97f), Map.entry(5L, 10.42f), Map.entry(6L, 845.49f), Map.entry(7L, 657.49f),
            Map.entry(8L, 99.19f), Map.entry(9L, 3434.00f), Map.entry(10L, 2157.19f), Map.entry(11L, 779421.31f),
            Map.entry(12L, 0f));

    @Autowired
    private KafkaProducer kafkaProducer;
    @Autowired
    private UserPopulator userPopulator;
    @Autowired
    private FileLoader fileLoader;
    @Autowired
    private TestRestTemplate rest;
    @MockBean
    private IncentiveClient incentiveClient;

    @Test
    void transactionsFromKafkaProduceTheExpectedBalances() {
        when(incentiveClient.incentiveFor(any())).thenReturn(0f);
        userPopulator.populate();
        for (String line : fileLoader.loadStrings("/test_data/rueiwoqp.tyruei")) {
            kafkaProducer.send(line);
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> EXPECTED.forEach((userId, expected) -> {
            Balance balance = rest.getForObject("/balance?userId=" + userId, Balance.class);
            assertThat(balance.getAmount()).as("user %d", userId).isCloseTo(expected, within(0.01f));
        }));
    }
}
