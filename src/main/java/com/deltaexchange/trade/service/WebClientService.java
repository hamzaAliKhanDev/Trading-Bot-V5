package com.deltaexchange.trade.service;

import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
@RequiredArgsConstructor
public class WebClientService {

    private final WebClient.Builder webClientBuilder;

    /**
     * Builds a WebClient using JVM default SSL configuration.
     * This works for all public HTTPS APIs (including Delta Exchange).
     */
    public WebClient buildClient(String baseUrl) {

        return webClientBuilder
                .baseUrl(baseUrl)
                .build();
    }
}
