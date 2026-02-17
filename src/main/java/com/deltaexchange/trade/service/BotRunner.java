package com.deltaexchange.trade.service;

import lombok.RequiredArgsConstructor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class BotRunner implements CommandLineRunner {
	@Autowired
	private StartBot startBot;

    @Override
    public void run(String... args) {
        System.out.println("[BOT] Starting continuous monitoring...");
        startBot.startBotMain();
    }
}
