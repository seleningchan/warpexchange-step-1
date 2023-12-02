package com.itranswarp.exchange.messaging;

public interface Messaging {
    enum Topic{
        SEQUENCE(1),
        TRANSFER(1),
        TRADE(1),
        TICK(1);
        private final int concurrency;
        Topic(int concurrency){
            this.concurrency=concurrency;
        }
        public int getConcurrency(){
            return this.concurrency;
        }
        public int getPartitions(){
            return this.concurrency;
        }
    }
}
