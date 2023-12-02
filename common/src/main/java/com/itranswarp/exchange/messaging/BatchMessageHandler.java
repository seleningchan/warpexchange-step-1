package com.itranswarp.exchange.messaging;

import com.itranswarp.exchange.message.AbstractMessage;

import java.util.List;

public interface BatchMessageHandler<T extends AbstractMessage> {
    void processMessages(List<T> messages);
}
