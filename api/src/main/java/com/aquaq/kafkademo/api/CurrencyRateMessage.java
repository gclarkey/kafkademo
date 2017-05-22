package com.aquaq.kafkademo.api;

import lombok.Data;

import java.time.LocalTime;

@Data
public class CurrencyRateMessage {

    private Long id;
    private String fromCurrency;
    private String toCurrency;
    private LocalTime time;
    private Double price1;
    private Double price2;
    private Double price3;
    private Double price4;

}
