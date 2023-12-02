package com.itranswarp.exchange.message.event;

import com.itranswarp.exchange.message.AbstractMessage;
import org.springframework.lang.Nullable;

public class AbstractEvent extends AbstractMessage {
    public long sequenceId;
    public long previousId;
    @Nullable
    public String uniqueId;
}
