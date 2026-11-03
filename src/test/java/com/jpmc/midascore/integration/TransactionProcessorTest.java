package com.jpmc.midascore.integration;

import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.TransactionProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Validation and balance rules, without Kafka (the listener container is not started). */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TransactionProcessorTest {

    @Autowired
    private TransactionProcessor processor;
    @Autowired
    private UserRepository users;
    @Autowired
    private TransactionRepository transactions;
    @MockBean
    private IncentiveClient incentiveClient;

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        alice = users.save(new UserRecord("alice", 100f)).getId();
        bob = users.save(new UserRecord("bob", 50f)).getId();
        when(incentiveClient.incentiveFor(any())).thenReturn(0f);
    }

    @Test
    void validTransactionMovesMoneyAndIsRecorded() {
        assertThat(processor.process(new Transaction(alice, bob, 30f))).isTrue();
        assertThat(users.findById(alice).getBalance()).isCloseTo(70f, within(1e-4f));
        assertThat(users.findById(bob).getBalance()).isCloseTo(80f, within(1e-4f));
        assertThat(transactions.count()).isEqualTo(1);
    }

    @Test
    void incentiveIsCreditedToRecipientOnly() {
        when(incentiveClient.incentiveFor(any())).thenReturn(5f);
        processor.process(new Transaction(alice, bob, 30f));
        assertThat(users.findById(alice).getBalance()).isCloseTo(70f, within(1e-4f));
        assertThat(users.findById(bob).getBalance()).isCloseTo(85f, within(1e-4f));
        assertThat(transactions.findAll().iterator().next().getIncentive()).isEqualTo(5f);
    }

    @Test
    void spendingExactlyTheWholeBalanceIsAllowed() {
        assertThat(processor.process(new Transaction(alice, bob, 100f))).isTrue();
        assertThat(users.findById(alice).getBalance()).isCloseTo(0f, within(1e-4f));
    }

    @Test
    void insufficientFundsAreRejected() {
        assertThat(processor.process(new Transaction(bob, alice, 50.01f))).isFalse();
        assertUnchanged();
    }

    @Test
    void unknownSenderOrRecipientIsRejected() {
        assertThat(processor.process(new Transaction(999L, bob, 1f))).isFalse();
        assertThat(processor.process(new Transaction(alice, 999L, 1f))).isFalse();
        assertUnchanged();
    }

    @Test
    void selfTransferAndNonPositiveAmountsAreRejected() {
        assertThat(processor.process(new Transaction(alice, alice, 10f))).isFalse();
        assertThat(processor.process(new Transaction(alice, bob, 0f))).isFalse();
        assertThat(processor.process(new Transaction(alice, bob, -5f))).isFalse();
        assertUnchanged();
    }

    private void assertUnchanged() {
        assertThat(users.findById(alice).getBalance()).isEqualTo(100f);
        assertThat(users.findById(bob).getBalance()).isEqualTo(50f);
        assertThat(transactions.count()).isZero();
        verify(incentiveClient, never()).incentiveFor(any());
    }
}
