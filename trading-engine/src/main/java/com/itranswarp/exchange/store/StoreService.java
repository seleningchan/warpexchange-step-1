package com.itranswarp.exchange.store;

import com.itranswarp.exchange.db.DbTemplate;
import com.itranswarp.exchange.message.event.AbstractEvent;
import com.itranswarp.exchange.messaging.MessageTypes;
import com.itranswarp.exchange.model.support.EntitySupport;
import com.itranswarp.exchange.model.trade.EventEntity;
import com.itranswarp.exchange.support.LoggerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Transactional
public class StoreService extends LoggerSupport {
    @Autowired
    MessageTypes messageTypes;
    @Autowired
    DbTemplate dbTemplate;

    public List<AbstractEvent> loadEventsFromDb(long lastEventId){
        var events = this.dbTemplate.from(EventEntity.class).where("sequenceid>?",lastEventId)
                .orderBy("sequenceId").limit(1000000).list();
        return events.stream().map(event ->(AbstractEvent)messageTypes.deserialize(event.data))
        .collect(Collectors.toList());
    }

    public void insertIgnore(List<? extends EntitySupport> list) {
        dbTemplate.insertIgnore(list);
    }



















}
