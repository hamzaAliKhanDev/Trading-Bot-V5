package com.deltaexchange.trade.service;

import com.deltaexchange.trade.config.DeltaConfig;
import com.deltaexchange.trade.util.DeltaSignatureUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PositionService {

    private static final Logger consoleLogger = LogManager.getLogger("Console");
    private static final Logger errorLogger = LogManager.getLogger("Error");

    @Autowired
    private WebClientService webClientService;
    @Autowired
    private DeltaConfig config;
    @Autowired
    private DeltaSignatureUtil signRequest;
    private final ObjectMapper mapper = new ObjectMapper();

    public Mono<JsonNode> getBTCPositionDetails() {
        try {
            String endpoint = "/v2/positions";
            String query = "product_id=" + config.getProductId();

            long timestamp = Instant.now().getEpochSecond();
            
            StringBuilder prehash = new StringBuilder();
            prehash.append("GET").append(timestamp).append(endpoint).append("?").append(query);
            String signature = signRequest.hmacSHA256(prehash.toString(), config.getApiSecret());

            StringBuilder endpointWithParams = new StringBuilder();
            endpointWithParams.append(endpoint).append("?").append(query);
            WebClient client = webClientService.buildClient(config.getBaseUrl());

            return client.get()
                    .uri(endpointWithParams.toString())
                    .header("api-key", config.getApiKey())
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(timestamp))
                    .header("Accept", "application/json")
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(response -> {
                        consoleLogger.info("Response of PositionDetailsService:::::{}", response);
                        try {
                            JsonNode json = mapper.readTree(response);
                            return json;
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to parse position details", e);
                        }
                    });
        } catch (Exception e) {
            errorLogger.error("Error occured in position service:::", e);
        }
        return null;
    }

}
