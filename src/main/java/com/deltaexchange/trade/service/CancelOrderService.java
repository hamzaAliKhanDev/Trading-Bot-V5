package com.deltaexchange.trade.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.deltaexchange.trade.config.DeltaConfig;
import com.deltaexchange.trade.util.DeltaSignatureUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class CancelOrderService {

    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");

    @Autowired
    private WebClientService webClientService;

    @Autowired
    private DeltaConfig config;

    @Autowired
    private DeltaSignatureUtil signRequest;

    private final ObjectMapper mapper = new ObjectMapper();

    public Mono<JsonNode> cancelOrdersForProductAsJson() {
    return cancelOrdersForProduct()
            .then(Mono.fromCallable(() -> {
                JSONObject obj = new JSONObject();
                obj.put("success", true);   // required for your business logic
                return mapper.readTree(obj.toString());
            }));
}

    // **************************************************************
    // PUBLIC METHOD → FIRST GET OPEN ORDERS → THEN CANCEL ONE BY ONE
    // **************************************************************
    public Mono<Void> cancelOrdersForProduct() {

        return getOpenOrders(Integer.valueOf(config.getProductId()))
                .flatMapMany(arr -> {
                    List<Integer> ids = new ArrayList<>();
                    for (int i = 0; i < arr.length(); i++) {
                        ids.add(arr.getJSONObject(i).getInt("id"));
                    }

                    if (ids.isEmpty()) {
                        consoleLogger.info("No open orders found for product_id={}", config.getProductId());
                        return Flux.empty();
                    }

                    consoleLogger.info("Found {} open orders. Cancelling...", ids.size());
                    return Flux.fromIterable(ids);
                })
                .flatMap(this::cancelSingleOrder)
                .then();
    }

    // **************************************************************
    // GET OPEN ORDERS FOR PRODUCT ID
    // **************************************************************
    private Mono<JSONArray> getOpenOrders(int productId) {
        try {
            String endpoint = "/v2/orders";
            String query = "state=open&product_id=" + productId;

            long ts = Instant.now().getEpochSecond();
            String fullEndpoint = endpoint + "?" + query;

            String prehash = "GET" + ts + fullEndpoint;
            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());

            consoleLogger.info("Fetching open orders from: {}", fullEndpoint);
            WebClient client = webClientService.buildClient(config.getBaseUrl());

            return client.get()
                    .uri(fullEndpoint)
                    .header("api-key", config.getApiKey())
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(ts))
                    .header("Accept", "application/json")
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(res -> {
                        consoleLogger.info("Open Orders Response: {}", res);
                        JSONObject json = new JSONObject(res);
                        return json.getJSONArray("result");
                    });

        } catch (Exception e) {
            errorLogger.error("Error fetching open orders:", e);
            return Mono.just(new JSONArray());
        }
    }

    // **************************************************************
    // CANCEL SINGLE ORDER EXACTLY LIKE YOUR WORKING CODE
    // **************************************************************
    public Mono<JsonNode> cancelSingleOrder(int orderId) {

        consoleLogger.info("Cancelling Order ID: {}", orderId);

        try {
            String endpoint = "/v2/orders";

            JSONObject body = new JSONObject();
            body.put("id", orderId);
            body.put("product_id", config.getProductId()); // 27

            long ts = Instant.now().getEpochSecond();
            String prehash = "DELETE" + ts + endpoint + body;
            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());

            WebClient client = webClientService.buildClient(config.getBaseUrl());

            return client.method(HttpMethod.DELETE)
                    .uri(endpoint)
                    .header("api-key", config.getApiKey())
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(ts))
                    .header("Content-Type", "application/json")
                    .bodyValue(body.toString())
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(res -> {
                        consoleLogger.info("Cancel Response for {} → {}", orderId, res);
                        try {
                            JsonNode json = mapper.readTree(res);
                            return json;
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to parse cancel orders response", e);
                        }
                    });

        } catch (Exception e) {
            errorLogger.error("Error cancelling order " + orderId, e);
            return Mono.empty();
        }
    }
}
