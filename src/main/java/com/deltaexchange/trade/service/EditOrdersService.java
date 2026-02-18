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
import com.deltaexchange.trade.service.SetLeverageService.ApiException;
import com.deltaexchange.trade.util.DeltaSignatureUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class EditOrdersService {

    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");
    private static final Logger transactionLogger = LogManager.getLogger("Transaction");

    @Autowired
    private WebClientService webClientService;

    @Autowired
    private DeltaConfig config;

    @Autowired
    private DeltaSignatureUtil signRequest;

    @Autowired
    private AddMarginService addMargin;

    private final ObjectMapper mapper = new ObjectMapper();

    // **************************************************************
    // PUBLIC METHOD → GET ALL ORDERS → FILTER LOT_SIZE=2 → EDIT THEM
    // **************************************************************
    public Mono<Void> editOrdersForLotSize(int size) {

        return getOpenOrders(Integer.valueOf(config.getProductId()))
                .flatMapMany(arr -> {

                    List<JSONObject> orderList = new ArrayList<>();

                    if (Math.abs(size) >= 960) {
                        String margin = "";
                        if (Math.abs(size) == 1920 || Math.abs(size) == 3840) {
                            margin = "250";
                        } else if (Math.abs(size) == 7680) {
                            margin = "750";
                        }
                        addMargin.addMargin(margin).subscribe(addMarginNode -> {
                            consoleLogger.info("addMarginNode:::::{}", addMarginNode);

                            JSONObject addMarginResponse = new JSONObject(addMarginNode.toString());
                            boolean apiSuccess = addMarginResponse.getBoolean("success");

                            if (!apiSuccess) {
                                consoleLogger.info(":::::::::AddMargin service returned success false::::::::::::");
                                return;
                            } else {
                                transactionLogger.info("AddMargin service call successfull for Size->{}:::::",
                                        size);
                            }
                        });
                    }

                    for (int i = 0; i < arr.length(); i++) {

                        JSONObject obj = arr.getJSONObject(i);

                        String side = obj.getString("side");

                        if (size > 0) {
                            if ("sell".equalsIgnoreCase(side)) {
                                orderList.add(obj);
                            }
                        } else {
                            if ("buy".equalsIgnoreCase(side)) {
                                orderList.add(obj);
                            }
                        }

                    }
                    consoleLogger.info("orderList::::{}", orderList);
                    if (orderList.isEmpty()) {
                        consoleLogger.info("No orders found with lot_size={} for product_id={}", size,
                                config.getProductId());
                        return Flux.empty();
                    }

                    consoleLogger.info("Found {} orders with lot_size={}. Editing...", size, orderList.size());
                    return Flux.fromIterable(orderList);
                })
                .flatMap(orderObj -> editSingleOrder(orderObj, size))
                .then();
    }

    // **************************************************************
    // GET OPEN ORDERS FOR PRODUCT ID (same as your existing code)
    // **************************************************************
    private Mono<JSONArray> getOpenOrders(int productId) {
        try {
            String endpoint = "/v2/orders";
            String query = "state=open&product_id=" + productId+"&limit="+config.getGetOrderLimit();

            long ts = Instant.now().getEpochSecond();
            String fullEndpoint = endpoint + "?" + query;

            String prehash = "GET" + ts + fullEndpoint;
            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());

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
    // EDIT SINGLE ORDER (NEW METHOD)
    // **************************************************************
    private Mono<JsonNode> editSingleOrder(JSONObject orderObj, int positionSize) {

        try {
            int orderId = orderObj.getInt("id");
            String side = orderObj.getString("side");
            // Based on Delta Exchange API → "buy" or "sell"

            double limitPrice = orderObj.getDouble("limit_price");

            double points = 500;
            double newPrice = side.equalsIgnoreCase("buy")
                    ? limitPrice + points
                    : limitPrice - points;

            transactionLogger.info("Editing Order ID: {} | side={} | oldPrice={} → newPrice={}",
                    orderId, side, limitPrice, newPrice);

            JSONObject body = new JSONObject();
            body.put("id", orderId);
            body.put("product_id", config.getProductId());
            body.put("limit_price", newPrice);
            body.put("size", Math.abs(positionSize) + 2);

            String endpoint = "/v2/orders";

            long ts = Instant.now().getEpochSecond();
            String prehash = "PUT" + ts + endpoint + body;
            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());

            WebClient client = webClientService.buildClient(config.getBaseUrl());

            return client.method(HttpMethod.PUT)
                    .uri(endpoint)
                    .header("api-key", config.getApiKey())
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(ts))
                    .header("Content-Type", "application/json")
                    .bodyValue(body.toString())
                    .exchangeToMono(response -> {
                        consoleLogger.info("Edit Response for {} → {}", orderId, response);
                       if (response.statusCode().is2xxSuccessful()) {
                            // success -> read body and convert to JsonNode
                            return response.bodyToMono(String.class)
                                    .map(responsebody -> {
                                        consoleLogger.info("Response of setOrderLeverage:::: {}", responsebody);
                                        try {
                                            return mapper.readTree(responsebody);
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
            errorLogger.error("Error editing order:", e);
            return Mono.empty();
        }
    }

}
