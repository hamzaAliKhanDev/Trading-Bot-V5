package com.deltaexchange.trade.model.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class PlaceOrderServiceRequest {

    @NotBlank(message = "product_symbol is mandatory")
    private String product_symbol;

    @NotBlank(message = "limit_price is mandatory")
    private String limit_price;

    @NotBlank(message = "side is mandatory")
    private String side;

    @NotBlank(message = "size is mandatory")
    private int size;

    @NotBlank(message = "order_type is mandatory")
    private  String order_type;
     
}
