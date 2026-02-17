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
public class SetLeverageService {
    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");

    @Autowired
    private WebClientService webClientService;
    @Autowired
    private DeltaConfig config;
    @Autowired
    private DeltaSignatureUtil signRequest;

    private final ObjectMapper mapper = new ObjectMapper();

    public Mono<JsonNode> setOrderLeverage(int leverage) {
        try {

            // EXACT SAME ENDPOINT AS WORKING CODE
            String endpoint = "/v2/products/" + config.getProductId() + "/orders/leverage";

            // BODY EXACT SAME AS WORKING CODE
            String bodyJson = "{\"leverage\":" + leverage + "}";

            long timestamp = Instant.now().getEpochSecond();

            consoleLogger.info("BodyJson String for Signature::: {}", bodyJson);

            // EXACT SAME PREHASH FORMAT: POST + timestamp + endpoint + body
            String prehash = "POST" + timestamp + endpoint + bodyJson;

            consoleLogger.info("Prehash::: {}", prehash);

            // SIGNATURE EXACTLY SAME
            String signature = signRequest.hmacSHA256(prehash, config.getApiSecret());

            WebClient client = webClientService.buildClient(config.getBaseUrl());

            consoleLogger.info("Final Endpoint:::: {}", endpoint);

           return client.post()
        .uri(endpoint)
        .header("api-key", config.getApiKey())
        .header("signature", signature)
        .header("timestamp", String.valueOf(timestamp))
        .header("Content-Type", "application/json")
        .bodyValue(bodyJson)
        .exchangeToMono(response -> {
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
                            consoleLogger.error("setOrderLeverage failed. status={} body={}", statusCode, errorBody);
                            errorLogger.error("setOrderLeverage failed. status={} body={}", statusCode, errorBody);
                            // Throw exception with exact status + body
                            return Mono.error(new ApiException(statusCode, errorBody));
                        });
            }
        });


        } catch (Exception e) {
            errorLogger.error("Error occurred in setOrderLeverage:::", e);
        }
        return null;
    }

    // custom runtime exception to carry status + body
public static class ApiException extends RuntimeException {
    private final int status;
    private final String body;

    public ApiException(int status, String body) {
        super("API error: status=" + status + ", body=" + body);
        this.status = status;
        this.body = body;
    }

    public int getStatus() { return status; }
    public String getBody() { return body; }
}

}
