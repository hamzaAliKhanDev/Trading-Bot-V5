package com.deltaexchange.trade.service;

import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.deltaexchange.trade.config.DeltaConfig;
import com.deltaexchange.trade.util.DeltaSignatureUtil;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class DeadZoneOrderService {

    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");

    @Autowired
    private CancelOrderService cancelOrderService;

    @Autowired
    private WebClientService webClientService;

    @Autowired
    private DeltaConfig config;

    @Autowired
    private DeltaSignatureUtil signRequest;

    @Autowired
    private CheckAndOrderService placeOrderService; // its method remains void

    // Polling / retry constants
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final int MAX_POLL_ATTEMPTS = 12; // 12 * 500ms = 6s
    private static final int MAX_LEVERAGE_RETRIES = 4;
    private static final Duration RETRY_WAIT_AFTER_INSUFFICIENT = Duration.ofSeconds(1);

    // **************************************************************
    // MAIN SERVICE FLOW
    // **************************************************************
    public Mono<Void> applyDeadZoneOrder() {

        return getOpenOrders(Integer.valueOf(config.getProductId()))
                .flatMap(arr -> {

                    JSONObject twoLotOrder = null;
                    List<Integer> toCancelList = new ArrayList<>();

                    for (int i = 0; i < arr.length(); i++) {
                        JSONObject obj = arr.getJSONObject(i);

                        int size = obj.getInt("size");
                        int id = obj.getInt("id");

                        if (size == 2) {
                            twoLotOrder = obj;   // save this one
                        } else {
                            toCancelList.add(id); // cancel all other orders
                        }
                    }

                    if (twoLotOrder == null) {
                        consoleLogger.info("No 2-lot order found. Nothing to reverse.");
                        return Mono.empty();
                    }

                    consoleLogger.info("2-lot order found: {}", twoLotOrder);

                    // Cancel other orders -> wait for cancellations to settle -> place opposite order
                    return cancelOtherOrders(toCancelList)
                            .then(waitUntilCancellationsSettled(Integer.valueOf(config.getProductId()), twoLotOrder.getInt("id")))
                            .then(placeOppositeOrder(twoLotOrder));
                })
                .then();
    }

    // **************************************************************
    // CANCEL ALL ORDERS EXCEPT LOT SIZE 2
    // **************************************************************
    private Mono<Void> cancelOtherOrders(List<Integer> ids) {

        if (ids.isEmpty()) {
            consoleLogger.info("No orders to cancel except the 2-lot order.");
            return Mono.empty();
        }

        consoleLogger.info("Cancelling {} orders...", ids.size());

        return Flux.fromIterable(ids)
                .flatMap(id -> cancelOrderService.cancelSingleOrder(id))
                .then();
    }

    // **************************************************************
    // WAIT UNTIL CANCELLATIONS ARE SETTLED
    // Resolves when either:
    //  - only the preservedOrderId remains (or)
    //  - there are zero open orders (if preservedOrderId < 0)
    // **************************************************************
    private Mono<Void> waitUntilCancellationsSettled(int productId, int preservedOrderId) {
        return Flux.interval(Duration.ZERO, POLL_INTERVAL)
                .take(MAX_POLL_ATTEMPTS)
                .flatMap(tick -> getOpenOrders(productId)
                        .onErrorResume(err -> {
                            errorLogger.warn("Error while polling open orders (will retry): {}", err.toString());
                            return Mono.just(new JSONArray()); // treat as empty and retry
                        }))
                .map(openArr -> {
                    int count = openArr.length();
                    if (preservedOrderId < 0) {
                        return count == 0;
                    }
                    // ensure no order other than preservedOrderId exists
                    for (int i = 0; i < openArr.length(); i++) {
                        JSONObject o = openArr.getJSONObject(i);
                        int id = o.getInt("id");
                        if (id != preservedOrderId) {
                            return false;
                        }
                    }
                    return true;
                })
                .filter(Boolean::booleanValue)
                .next()
                .switchIfEmpty(Mono.fromRunnable(() ->
                        consoleLogger.warn("Timeout waiting for cancellations to settle. Proceeding anyway.")))
                .then();
    }

    // **************************************************************
    // PLACE OPPOSITE ORDER 1250 POINTS AWAY
    // - returns Mono<Void>
    // - wraps the existing void changeLevAndexecuteOrder with a Mono
    // - retries on likely insufficient-margin errors
    // **************************************************************
    private Mono<Void> placeOppositeOrder(JSONObject twoLotOrder) {

        try {
            double limitPrice = twoLotOrder.getDouble("limit_price");
            String side = twoLotOrder.getString("side");

            String oppositeSide = side.equalsIgnoreCase("buy") ? "sell" : "buy";

            double newLimitPrice = side.equalsIgnoreCase("buy")
                    ? (limitPrice + 1250)
                    : (limitPrice - 1250);

            consoleLogger.info(
                    "Placing opposite order | originalSide={} | newSide={} | oldPrice={} | newPrice={}",
                    side, oppositeSide, limitPrice, newLimitPrice
            );

            return attemptChangeLeverageAndPlaceWithRetries(
                    10,
                    String.valueOf(newLimitPrice),
                    2,
                    oppositeSide,
                    MAX_LEVERAGE_RETRIES,
                    Integer.valueOf(config.getProductId())
            );

        } catch (Exception e) {
            errorLogger.error("Error preparing opposite order:", e);
            return Mono.empty();
        }
    }

    // **************************************************************
    // WRAPPER: convert the existing void method into Mono<Void>
    // using Mono.defer + Mono.fromRunnable so it executes when subscribed.
    // Runs on boundedElastic to avoid blocking reactor threads.
    // **************************************************************
 private Mono<Void> callChangeLevAndExecuteOrderAsMono(int leverage, String price, int size, String side) {

    return Mono.defer(() ->
        Mono.<Void>fromCallable(() -> {
            placeOrderService.changeLevAndexecuteOrder(leverage, price, size, side);
            return null;
        })
    ).subscribeOn(Schedulers.boundedElastic());
}


    // **************************************************************
    // RETRY WRAPPER: attempts callChangeLevAndExecuteOrderAsMono and
    // retries when the error looks like "insufficient margin".
    // **************************************************************
    private Mono<Void> attemptChangeLeverageAndPlaceWithRetries(int leverage,
                                                                String price,
                                                                int size,
                                                                String side,
                                                                int remainingRetries,
                                                                int productId) {

        return callChangeLevAndExecuteOrderAsMono(leverage, price, size, side)
                .doOnSuccess(v -> consoleLogger.info("Successfully changed leverage and placed order (wrapped)"))
                .doOnError(err -> errorLogger.error("Error in changeLevAndexecuteOrder (wrapped): {}", err.toString()))
                .onErrorResume(err -> {
                    if (isInsufficientMarginError(err) && remainingRetries > 0) {
                        consoleLogger.warn("Detected insufficient-margin error when setting leverage (attempts left={}). Will wait and retry. Error: {}",
                                remainingRetries - 1, err.toString());

                        return Mono.delay(RETRY_WAIT_AFTER_INSUFFICIENT)
                                .then(waitUntilCancellationsSettled(productId, -1)) // re-check open orders/reserved margin
                                .then(attemptChangeLeverageAndPlaceWithRetries(leverage, price, size, side, remainingRetries - 1, productId));
                    } else {
                        // No recovery or retries exhausted
                        errorLogger.error("Failed to set leverage/place order. error: {}", err.toString());
                        return Mono.error(err);
                    }
                });
    }

    // **************************************************************
    // SIMPLE HEURISTIC TO DETECT "INSUFFICIENT MARGIN" ERRORS
    // Adjust checks to the actual API error body you get.
    // **************************************************************
    private boolean isInsufficientMarginError(Throwable ex) {
        if (ex == null) return false;

        String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";

        if (msg.contains("insufficient") || msg.contains("insufficient_margin")
                || msg.contains("no margin") || msg.contains("not enough margin")
                || msg.contains("margin is too low")) {
            return true;
        }

        // Sometimes the HTTP client wraps status/text into message
        if (msg.contains("400") && (msg.contains("margin") || msg.contains("insufficient"))) {
            return true;
        }

        // also inspect causes
        Throwable cause = ex.getCause();
        while (cause != null) {
            String cmsg = cause.getMessage() != null ? cause.getMessage().toLowerCase() : "";
            if (cmsg.contains("insufficient") || cmsg.contains("margin")) return true;
            cause = cause.getCause();
        }

        return false;
    }

    // **************************************************************
    // GET OPEN ORDERS (unchanged)
    // **************************************************************
    private Mono<JSONArray> getOpenOrders(int productId) {
        try {
            String endpoint = "/v2/orders";
            String query = "state=open&product_id=" + productId;

            long ts = Instant.now().getEpochSecond();
            String fullEndpoint = endpoint + "?" + query;

            String prehash = "GET" + ts + fullEndpoint;
            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());

            var client = webClientService.buildClient(config.getBaseUrl());

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
}
