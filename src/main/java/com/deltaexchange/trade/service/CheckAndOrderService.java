package com.deltaexchange.trade.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.deltaexchange.trade.config.DeltaDto;
import com.fasterxml.jackson.databind.JsonNode;

import reactor.core.publisher.Mono;

@Service
public class CheckAndOrderService {

        @Autowired
        private CancelOrderService cancelAllOrders;
        @Autowired
        private SetLeverageService setOrderLeverage;
        @Autowired
        private PlaceOrderService placeOrder;

        @Autowired
        private DeltaDto deltaDto;

        @Autowired
        private EditOrdersService editOrders;

        @Autowired
        private AddMarginService addMargin;

        private static final Logger consoleLogger = LogManager.getLogger("Console");
        private static final Logger errorLogger = LogManager.getLogger("Error");
        private static final Logger transactionLogger = LogManager.getLogger("Transaction");

        public void executionMain(String entryPrice, int size) {

                try {
                        // Add Margin added for case 1 and 36 start
                        if(Math.abs(size)==2 || Math.abs(size)==60){
                                String margin = "";
                                if(Math.abs(size)==2){
                                        margin="15";
                                }else{
                                        margin="15";
                                }
                               boolean addMarginResponse = addMarginSync(margin);
                               if(!addMarginResponse){
                                        consoleLogger.info("AddMargin service returned success=false for case 1");
                                        return;
                               }
                                // Add Margin added for case 1 and 36 End
                        }      

                        // ====== 1️⃣ CANCEL ALL ORDERS (BLOCKING) ======
                        if (Math.abs(size) == 2) {

                                int leverage = returnLeverage(size);

                                JsonNode cancelOrdersNode = cancelAllOrders.cancelOrdersForProductAsJson().block();

                                JSONObject cancelOrdersResponse = new JSONObject(cancelOrdersNode.toString());

                                if (!cancelOrdersResponse.getBoolean("success")) {
                                        consoleLogger.info("Cancel Order service returned success=false");
                                        return;
                                }

                                transactionLogger.info(
                                                "Cancelled all previous orders for EntryPrice={}, Size={}",
                                                entryPrice, size);

                                // ====== 2️⃣ SET LEVERAGE (BLOCKING) ======
                                JsonNode setLeverageNode = setOrderLeverage.setOrderLeverage(leverage).block();

                                JSONObject setLeverageResponse = new JSONObject(setLeverageNode.toString());

                                if (!setLeverageResponse.getBoolean("success")) {
                                        consoleLogger.info("Set Leverage service returned success=false");
                                        return;
                                }

                                transactionLogger.info(
                                                "Leverage set successfully. EntryPrice={}, Size={}, Leverage={}",
                                                entryPrice, size, leverage);

                                // ====== 3️⃣ PLACE ORDERS (BLOCKING & SEQUENTIAL) ======
                                placeOrder(entryPrice, size);
                        }

                        // ====== 4️⃣ EDIT ORDERS (BLOCKING) ======
                        if (Math.abs(size) >= 4) {
                                editOrders.editOrdersForLotSize(size).block();
                        }

                        deltaDto.setLastOrderSize(size);

                } catch (Exception e) {
                        errorLogger.error("Error occured in Check and Order Service:::::", e);
                }
        }

        private boolean addMarginSync(String margin) {
                JsonNode addMarginNode = addMargin.addMargin(margin).block();

                consoleLogger.info("addMarginNode:::::{}", addMarginNode);

                if (addMarginNode == null) {
                        consoleLogger.error("AddMargin API returned null response");
                        return false;
                }

                JSONObject addMarginResponse = new JSONObject(addMarginNode.toString());
                boolean apiSuccess = addMarginResponse.getBoolean("success");

                if (!apiSuccess) {
                        consoleLogger.info(
                                        ":::::::::AddMargin service returned success false::::::::::::");
                        return false;
                }

                transactionLogger.info(
                                "AddMargin service call successful for Size->2 or 72:::::");

                return true;
        }

        public int returnLeverage(int size) {

                int abs = Math.abs(size);

                switch (abs) {
                        case 2:
                        case 4:
                        case 12:
                                return 10;

                        case 24:
                                return 25;

                        default:
                                return 10;
                }
        }

        public void placeOrder(String entryPrice, int size) {

                double entryPriceRaw = Double.parseDouble(entryPrice);
                long entryPriceDouble = (long) entryPriceRaw;

                switch (size) {

                        case 2:
                                executeOrder(String.valueOf(entryPriceDouble + 500), 4, "sell");
                                executeOrder(String.valueOf(entryPriceDouble - 750), 2, "buy");
                                executeOrder(String.valueOf(entryPriceDouble - 1250), 8, "buy");
                                changeLevAndexecuteOrder(20, String.valueOf(entryPriceDouble - 1750), 12, "buy");
                                changeLevAndexecuteOrder(20, String.valueOf(entryPriceDouble - 2250), 36, "buy");
                                changeLevAndexecuteOrder(30, String.valueOf(entryPriceDouble - 2750), 60, "buy");
                                changeLevAndexecuteOrder(30, String.valueOf(entryPriceDouble - 3250), 120, "buy");
                                changeLevAndexecuteOrder(40, String.valueOf(entryPriceDouble - 3750), 240, "buy");
                                changeLevAndexecuteOrder(40, String.valueOf(entryPriceDouble - 4250), 480, "buy");
                                changeLevAndexecuteOrder(50, String.valueOf(entryPriceDouble - 4750), 960, "buy");
                                changeLevAndexecuteOrder(60, String.valueOf(entryPriceDouble - 5250), 1920, "buy");
                                changeLevAndexecuteOrder(60, String.valueOf(entryPriceDouble - 5750), 3840, "buy");

                                break;

                        case -2:
                                executeOrder(String.valueOf(entryPriceDouble - 500), 4, "buy");
                                executeOrder(String.valueOf(entryPriceDouble + 750), 2, "sell");
                                executeOrder(String.valueOf(entryPriceDouble + 1250), 8, "sell");
                                changeLevAndexecuteOrder(20, String.valueOf(entryPriceDouble + 1750), 12, "sell");
                                changeLevAndexecuteOrder(20, String.valueOf(entryPriceDouble + 2250), 36, "sell");
                                changeLevAndexecuteOrder(30, String.valueOf(entryPriceDouble + 2750), 60, "sell");
                                changeLevAndexecuteOrder(30, String.valueOf(entryPriceDouble + 3250), 120, "sell");
                                changeLevAndexecuteOrder(40, String.valueOf(entryPriceDouble + 3750), 240, "sell");
                                changeLevAndexecuteOrder(40, String.valueOf(entryPriceDouble + 4250), 480, "sell");
                                changeLevAndexecuteOrder(50, String.valueOf(entryPriceDouble + 4750), 960, "sell");
                                changeLevAndexecuteOrder(60, String.valueOf(entryPriceDouble + 5250), 1920, "sell");
                                changeLevAndexecuteOrder(60, String.valueOf(entryPriceDouble + 5750), 3840, "sell");
                                break;

                }
        }

        public void executeOrder(String limitPrice, int size, String side) {

                try {
                        JsonNode placeOrderNode = placeOrder.placeOrder(limitPrice, size, side).block();

                        if (placeOrderNode == null) {
                                consoleLogger.error(
                                                "Place Order returned null. LimitPrice={}, Size={}, Side={}",
                                                limitPrice, size, side);
                                return;
                        }

                        JSONObject placeOrderResponse = new JSONObject(placeOrderNode.toString());
                        boolean apiSuccess = placeOrderResponse.getBoolean("success");

                        if (!apiSuccess) {
                                consoleLogger.info(
                                                "Place Order service returned success=false. LimitPrice={}, Size={}, Side={}",
                                                limitPrice, size, side);
                                return;
                        }

                        transactionLogger.info(
                                        "Order Placed Successfully. Side={}, LimitPrice={}, Size={}",
                                        side, limitPrice, size);

                        transactionLogger.info(
                                        ":::::::::::::::::::: New Order execution completed :::::::::::::::::");

                } catch (Exception e) {
                        errorLogger.error(
                                        "Error placing order synchronously. LimitPrice={}, Size={}, Side={}",
                                        limitPrice, size, side, e);
                }
        }

        public void changeLevAndexecuteOrder(int leverage, String limitPrice, int size, String side) {

                try {
                        // 1️⃣ SET LEVERAGE (BLOCKING)
                        JsonNode setLeverageNode = setOrderLeverage.setOrderLeverage(leverage).block();

                        JSONObject setLeverageResponse = new JSONObject(setLeverageNode.toString());
                        boolean apiSuccesslev = setLeverageResponse.getBoolean("success");

                        if (!apiSuccesslev) {
                                consoleLogger.info("Set Leverage Service returned success=false");
                                return;
                        }

                        transactionLogger.info(
                                        "Leverage Set Successfully. limitPrice={}, size={}, leverage={}",
                                        limitPrice, size, leverage);

                        // 2️⃣ PLACE ORDER (BLOCKING)
                        JsonNode placeOrderNode = placeOrder.placeOrder(limitPrice, size, side).block();

                        JSONObject placeOrderResponse = new JSONObject(placeOrderNode.toString());
                        boolean apiSuccess = placeOrderResponse.getBoolean("success");

                        if (!apiSuccess) {
                                consoleLogger.info(
                                                "Place Order service returned false. limitPrice={}, size={}, side={}",
                                                limitPrice, size, side);
                                return;
                        }

                        transactionLogger.info(
                                        "Order Placed Successfully. side={}, limitPrice={}, size={}",
                                        side, limitPrice, size);

                        transactionLogger.info(
                                        ":::::::::::::::::: Order execution completed ::::::::::::::::::");

                } catch (Exception e) {
                        errorLogger.error("Error in changeLevAndexecuteOrder", e);
                }
        }

}
