package org.mpisws.hitmc.server;

import org.mpisws.hitmc.api.state.ClientRequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClientRequestEvent extends AbstractEvent{
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestEvent.class);

    private final ClientRequestType type;
    private String data;

    public ClientRequestEvent(final int id, ClientRequestType type, ClientRequestExecutor eventExecutor) {
        super(id, eventExecutor);
        this.type = type;
        this.data = "-1";
    }

    public ClientRequestType getType() {
        return type;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        return "ClientRequestEvent{" +
                "id=" + getId() +
                ", type=" + getType() +
                ", data=" + getData() +
                "}";
    }
}
