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
public class AddMarginService {

    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");

    @Autowired
    private WebClientService webClientService;
    @Autowired
    private DeltaConfig config;
    @Autowired
    private DeltaSignatureUtil signRequest;

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Overload: accept custom amount (in USD).
     */
    public Mono<JsonNode> addMargin(String deltaMarginStr) {
        try {
            String endpoint = "/v2/positions/change_margin";

            String bodyJson =
                    "{\"product_id\":" + config.getProductId() +
                    ",\"delta_margin\":\"" + deltaMarginStr + "\"}";

            long timestamp = Instant.now().getEpochSecond();

            consoleLogger.info("Add Margin Body JSON::: {}", bodyJson);

            String prehash = "POST" + timestamp + endpoint + bodyJson;
            consoleLogger.info("Add Margin Prehash::: {}", prehash);

            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());
            consoleLogger.info("Add Margin Signature::: {}", signature);

            WebClient client = webClientService.buildClient(config.getBaseUrl());

            return client.post()
                    .uri(endpoint)
                    .header("api-key", config.getApiKey())
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(timestamp))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .bodyValue(bodyJson)
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(response -> {
                        consoleLogger.info("Response of AddMargin Service:::: {}", response);
                        try {
                            return mapper.readTree(response);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to parse AddMargin Service response", e);
                        }
                    });

        } catch (Exception e) {
            errorLogger.error("Error occurred in AddMargin Service:::", e);
        }
        return null;
    }
}
