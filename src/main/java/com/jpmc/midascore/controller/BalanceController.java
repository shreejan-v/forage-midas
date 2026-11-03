package com.jpmc.midascore.controller;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {
    private final UserRepository users;

    public BalanceController(UserRepository users) {
        this.users = users;
    }

    /** Balance of a user; unknown users have a balance of 0. */
    @GetMapping("/balance")
    public Balance balance(@RequestParam long userId) {
        UserRecord user = users.findById(userId);
        return new Balance(user == null ? 0f : user.getBalance());
    }
}
