package com.jpmc.midascore.integration;

import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.foundation.Transaction;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.client.MockServerRestTemplateCustomizer;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/** The incentive API is replaced by a mock HTTP server; the real Forage jar is not needed. */
class IncentiveClientTest {
    private static final String URL = "http://incentive.test/incentive";

    @Test
    void postsTheTransactionAndReadsTheIncentive() {
        MockServerRestTemplateCustomizer server = new MockServerRestTemplateCustomizer();
        IncentiveClient client = new IncentiveClient(new RestTemplateBuilder(server), URL);
        server.getServer().expect(requestTo(URL)).andExpect(method(HttpMethod.POST))
                .andExpect(jsonPath("$.senderId").value(1)).andExpect(jsonPath("$.amount").value(12.5))
                .andRespond(withSuccess("{\"amount\": 3.75}", MediaType.APPLICATION_JSON));

        assertThat(client.incentiveFor(new Transaction(1, 2, 12.5f))).isEqualTo(3.75f);
        server.getServer().verify();
    }

    @Test
    void failsSafeToZeroWhenTheApiErrors() {
        MockServerRestTemplateCustomizer server = new MockServerRestTemplateCustomizer();
        IncentiveClient client = new IncentiveClient(new RestTemplateBuilder(server), URL);
        server.getServer().expect(requestTo(URL)).andRespond(withServerError());

        assertThat(client.incentiveFor(new Transaction(1, 2, 12.5f))).isZero();
    }
}
