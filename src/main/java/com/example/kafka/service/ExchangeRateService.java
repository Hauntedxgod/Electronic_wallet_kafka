package com.example.kafka.service;

import com.example.kafka.domain.ActualCurrencyRate;
import com.example.kafka.domain.CurrencyRate;
import com.example.kafka.kafka.KafkaProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
@EnableScheduling
public class ExchangeRateService {

    private final List<CurrencyRate> rates;
    private final KafkaProducer kafkaProducer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExchangeRateService(KafkaProducer kafkaProducer, @Value("${stock.exchange-rates}") String ratesParam) throws JsonProcessingException {
        this.kafkaProducer = kafkaProducer;
        rates = objectMapper.readValue(ratesParam, new TypeReference<>() {});
    }

    @Scheduled(fixedRateString = "${stock.calc-exchange-rate-every-number-of-seconds}000")
    public void sendRatesToKafka() throws JsonProcessingException {
        List<ActualCurrencyRate> actualCurrencyRates = new ArrayList<>();
        for (CurrencyRate currencyRate: rates) {
            actualCurrencyRates.add(ActualCurrencyRate.builder()
                    .currency(currencyRate.getCurrency())
                    .rate(generateRandomRate(currencyRate))
                    .build());
        }
        System.out.println(objectMapper.writeValueAsString(actualCurrencyRates));
        kafkaProducer.sendMessage(objectMapper.writeValueAsString(actualCurrencyRates));
    }

    private BigDecimal generateRandomRate(CurrencyRate currencyRate) {
        Random random = new Random();
        double range = currencyRate.getRateTo().subtract(currencyRate.getRateFrom()).doubleValue();
        return BigDecimal.valueOf(random.nextDouble(range) + currencyRate.getRateFrom().doubleValue()).setScale(2, RoundingMode.HALF_UP);
    }

}

