package com.jpmc.midascore.service;

import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Validates a transaction and, if valid, moves the money atomically.
 * A transaction is rejected when the sender or recipient does not exist, sender and recipient are the
 * same user, the amount is not positive, or the sender balance cannot cover the amount.
 * The recipient receives the amount plus the incentive; the sender is debited only the amount.
 */
@Service
public class TransactionProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessor.class);

    private final UserRepository users;
    private final TransactionRepository transactions;
    private final IncentiveClient incentiveClient;

    public TransactionProcessor(UserRepository users, TransactionRepository transactions, IncentiveClient incentiveClient) {
        this.users = users;
        this.transactions = transactions;
        this.incentiveClient = incentiveClient;
    }

    /** Returns true if the transaction was applied, false if it was rejected. */
    @Transactional
    public boolean process(Transaction t) {
        UserRecord sender = users.findById(t.getSenderId());
        UserRecord recipient = users.findById(t.getRecipientId());
        if (sender == null || recipient == null) {
            logger.info("rejected (unknown user): {}", t);
            return false;
        }
        if (sender.getId().equals(recipient.getId()) || t.getAmount() <= 0 || sender.getBalance() < t.getAmount()) {
            logger.info("rejected (invalid amount or insufficient funds): {}", t);
            return false;
        }
        float incentive = incentiveClient.incentiveFor(t);
        sender.setBalance(sender.getBalance() - t.getAmount());
        recipient.setBalance(recipient.getBalance() + t.getAmount() + incentive);
        users.save(sender);
        users.save(recipient);
        transactions.save(new TransactionRecord(sender, recipient, t.getAmount(), incentive));
        logger.info("applied {} with incentive {}", t, incentive);
        return true;
    }
}
