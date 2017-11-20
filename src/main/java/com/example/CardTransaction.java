package com.example;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * @author Podshivalov N.A.
 * @since 18.11.2017.
 */
@Getter
@AllArgsConstructor
class CardTransaction {

    private String payer;
    private String recipient;
    private LocalDateTime createdAt;
    private BigDecimal amount;

}
