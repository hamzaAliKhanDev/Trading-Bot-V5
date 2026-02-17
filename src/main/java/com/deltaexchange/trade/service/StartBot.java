package com.deltaexchange.trade.service;

import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.deltaexchange.trade.config.DeltaConfig;
import com.deltaexchange.trade.config.DeltaDto;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class StartBot {

	@Autowired
	private PositionService positionService;

	@Autowired
	private DeltaConfig config;
	
	@Autowired
	private CheckAndOrderService placeOrder;
	
	@Autowired
	private DeltaDto deltaDto;
	
	@Autowired
	private DeadZoneOrderService deadZoneOrder;

	private static final Logger consoleLogger = LogManager.getLogger("Console");
	private static final Logger errorLogger = LogManager.getLogger("Error");

	public void startBotMain() {

    consoleLogger.info(":::::::::::::::: Bot Started :::::::::::::");

    Mono.defer(this::runSingleCycle)
        .repeat()   // repeat AFTER completion
        .subscribe();
}
private Mono<Void> runSingleCycle() {

    return Mono.delay(Duration.ofSeconds(config.getLoopInterval()))
        .then(
            positionService.getBTCPositionDetails()
                .publishOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .doOnNext(position -> {

                    consoleLogger.info("[BOT] Cycle started");

                    if (position == null) {
                        consoleLogger.info("[BOT] No position data");
                        return;
                    }

                    JSONObject response = new JSONObject(position.toString());

                    if (!response.getBoolean("success")) {
                        consoleLogger.info("[BOT] Position API returned success=false");
                        return;
                    }

                    JSONObject result = response.optJSONObject("result");
                    if (result == null || result.isEmpty()) {
                        consoleLogger.info("[BOT] Empty result");
                        return;
                    }

                    long entryPrice =
                            (long) Double.parseDouble(result.optString("entry_price", "0"));

                    int size = result.getInt("size");

                    consoleLogger.info(
                            "LastOrderSize={} CurrentOrderSize={}",
                            deltaDto.getLastOrderSize(), size);

                    if (size != deltaDto.getLastOrderSize()) {
                        // ✅ fully synchronous execution
                        placeOrder.executionMain(String.valueOf(entryPrice), size);
                    } else {
                        consoleLogger.info("[BOT] Orders already placed");
                    }

                    consoleLogger.info("[BOT] Cycle completed");
                })
                .then()
        );
}

}
