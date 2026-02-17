package com.deltaexchange.trade.service;

import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.deltaexchange.trade.config.DeltaConfig;
import com.deltaexchange.trade.util.DeltaSignatureUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

@Service
public class GetOpenOrdersService {

    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");

    @Autowired
    private WebClientService webClientService;
    @Autowired
    private DeltaConfig config;
    @Autowired
    private DeltaSignatureUtil signatureUtil;

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * GET /v2/orders?state=open&product_ids=27
     */
    public Mono<JsonNode> getOpenOrdersForBTC() {

        try {
            String endpoint = "/v2/orders";

            // Query params for product BTC futures product_id=27
            String query = "?product_ids="+config.getProductId();

            long timestamp = Instant.now().getEpochSecond();

            // -------------------------------------------------------------
            // SIGNATURE = METHOD + TIMESTAMP + ENDPOINT + QUERY + BODY
            // For GET request body is "" (empty)
            // -------------------------------------------------------------
            String prehash = "GET" + timestamp + endpoint + query;

            String signature = signatureUtil.hmacSHA256(prehash, config.getApiSecret());

            WebClient client = webClientService.buildClient(config.getBaseUrl());

            return client.get()
                    .uri(endpoint + query)
                    .header("api-key", config.getApiKey())
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(timestamp))
                    .header("Accept", "application/json")
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(response -> {
                        consoleLogger.info("Open Orders Response:::::{}", response);

                        try {
                            return mapper.readTree(response);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to parse open orders response", e);
                        }
                    })
                    .onErrorResume(err -> {
                        errorLogger.error("Error while fetching open orders::::", err);
                        return Mono.empty();
                    });

        } catch (Exception ex) {
            errorLogger.error("Unexpected Error in getOpenOrdersForBTC()", ex);
            return Mono.empty();
        }
    }
}
