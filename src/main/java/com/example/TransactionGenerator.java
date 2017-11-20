package com.example;

import lombok.val;

import javax.security.auth.login.AccountException;
import javax.swing.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Podshivalov N.A.
 * @since 18.11.2017.
 */
public class TransactionGenerator {
    private static final int DEFAULT_CACHE_SIZE = 10;
    private static final MathContext ROUND_RULE = new MathContext(2, RoundingMode.HALF_UP);
    private static final Random random = new Random();
    private String[] accountCache;

    public TransactionGenerator(){
        doAccountCache(DEFAULT_CACHE_SIZE);
    }

    public TransactionGenerator(int cacheSize){
        doAccountCache(cacheSize);
    }


    Collection<CardTransaction> generate(int num){
        int cacheSize = accountCache.length;
        val now = LocalDateTime.now();
        val transactions = new ArrayList<CardTransaction>(num);
        while (num-- >= 0){
            val payer = accountCache[random.nextInt(cacheSize)];
            String recipient;
            do {
                recipient = accountCache[random.nextInt(cacheSize)];
            }while (!payer.equals(recipient));

            val createdAt =  now.minusHours(random.nextInt(23))
                    .minusMinutes(random.nextInt(60))
                    .minusSeconds(random.nextInt(60));

            val transaction = new CardTransaction(payer, recipient, createdAt, randomAmount());
            transactions.add(transaction);
        }
        return transactions;
    }

    private BigDecimal randomAmount(){
        return BigDecimal.valueOf(random.nextDouble() * 100).round(ROUND_RULE);
    }

    private void doAccountCache(int size){
        accountCache = new String[size];
        while (size-- > 0){
            accountCache[size] =  generateAccountNum().toString();
        }
    }


    private Long generateAccountNum(){
        return ((long) (random.nextDouble() * 100000000000000L));
    }
}
