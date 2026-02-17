package com.deltaexchange.trade.service;

import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.deltaexchange.trade.config.DeltaConfig;
import com.deltaexchange.trade.service.SetLeverageService.ApiException;
import com.deltaexchange.trade.util.DeltaSignatureUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

@Service
public class PlaceOrderService {

    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");

    @Autowired
    private WebClientService webClientService;
    @Autowired
    private DeltaConfig config;
    @Autowired
    private DeltaSignatureUtil signRequest;

    private final ObjectMapper mapper = new ObjectMapper();

    public Mono<JsonNode> placeOrder(String limitPrice, int size, String side) {
        try {
            String endpoint = "/v2/orders";

            // EXACT deterministic JSON (required for signature)
            String bodyJson = "{\"product_id\":" + config.getProductId() +
                    ",\"product_symbol\":\"" + config.getSymbol() + "\"" +
                    ",\"limit_price\":" + limitPrice +
                    ",\"size\":" + size +
                    ",\"side\":\"" + side + "\"" +
                    ",\"order_type\":\"limit_order\"}";

            long timestamp = Instant.now().getEpochSecond();

            consoleLogger.info("Order Body JSON::: {}", bodyJson);

            // PREHASH: EXACT FORMAT REQUIRED BY DELTA INDIA
            String prehash = "POST" + timestamp + endpoint + bodyJson;
            consoleLogger.info("Order Prehash::: {}", prehash);

            // SIGNATURE
            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());
            consoleLogger.info("Order Signature::: {}", signature);

            WebClient client = webClientService.buildClient(config.getBaseUrl());

            return client.post()
                    .uri(endpoint)
                    .header("api-key", config.getApiKey())
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(timestamp))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .bodyValue(bodyJson) // RAW STRING BODY (important)
                    .exchangeToMono(response -> {
                        consoleLogger.info("Response of Order Service:::: {}", response);
                        if (response.statusCode().is2xxSuccessful()) {
                            // success -> read body and convert to JsonNode
                            return response.bodyToMono(String.class)
                                    .map(body -> {
                                        consoleLogger.info("Response of setOrderLeverage:::: {}", body);
                                        try {
                                            return mapper.readTree(body);
                                        } catch (Exception e) {
                                            throw new RuntimeException("Failed to parse setOrderLeverage response", e);
                                        }
                                    });
                        } else {
                            // error -> read full error body and throw a custom exception containing it
                            return response.bodyToMono(String.class)
                                    .defaultIfEmpty("") // defensive: server might return empty body
                                    .flatMap(errorBody -> {
                                        int statusCode = response.rawStatusCode();
                                        consoleLogger.error("PlaceOrder API failed. status={} body={}", statusCode,
                                                errorBody);
                                        errorLogger.error("Place Order API failed. status={} body={}", statusCode,
                                                errorBody);
                                        // Throw exception with exact status + body
                                        return Mono.error(new ApiException(statusCode, errorBody));
                                    });
                        }
                    });

        } catch (Exception e) {
            errorLogger.error("Error occurred in Order Service:::", e);
        }
        return null;
    }
}
