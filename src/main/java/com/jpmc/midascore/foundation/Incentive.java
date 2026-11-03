package com.jpmc.midascore.foundation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Response of the incentive API: the bonus credited to the recipient of a transaction. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Incentive {
    private float amount;

    public Incentive() {
    }

    public Incentive(float amount) {
        this.amount = amount;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }
}
