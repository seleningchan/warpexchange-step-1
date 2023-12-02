package com.itranswarp.exchange.assets;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.math.BigDecimal;

public class Asset {
    BigDecimal available;
    BigDecimal frozen;

    public Asset(){
        this(BigDecimal.ZERO, BigDecimal.ZERO);
    }

    public  Asset(BigDecimal available, BigDecimal frozen){
        this.available=available;
        this.frozen=frozen;
    }
    public BigDecimal getAvailable(){
        return this.available;
    }
    public BigDecimal getFrozen(){
        return this.frozen;
    }

    @JsonIgnore
    public BigDecimal getTotal(){
        return available.add(frozen);
    }

    public  String toString(){
        return String.format("[available=%04.2f, frozen=%02.2f]", available,frozen);
    }
}
