package com.deltaexchange.trade.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Service
@Getter
@Configuration
public class DeltaConfig {

    @Value("${delta.base.url}")
    private String baseUrl;

    @Value("${delta.symbol}")
    private String symbol;

    @Value("${delta.order.size}")
    private double orderSize;

    @Value("${delta.loop.interval}")
    private int loopInterval;

    @Value("${delta.api.key}")
    private String apiKey;

    @Value("${delta.api.secret}")
    private String apiSecret;

     @Value("${delta.api.productid}")
    private String productId;
}

