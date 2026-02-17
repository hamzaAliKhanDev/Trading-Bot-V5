package com.deltaexchange.trade.config;

import org.springframework.stereotype.Service;

@Service
public class DeltaDto {
	
	private int lastOrderSize = 0 ;
	private boolean deadZoneOrderPlaced = false ;

	public boolean isDeadZoneOrderPlaced() {
		return deadZoneOrderPlaced;
	}

	public void setDeadZoneOrderPlaced(boolean deadZoneOrderPlaced) {
		this.deadZoneOrderPlaced = deadZoneOrderPlaced;
	}

	public int getLastOrderSize() {
		return lastOrderSize;
	}

	public void setLastOrderSize(int lastOrderSize) {
		this.lastOrderSize = lastOrderSize;
	} 
}
