package org.mpisws.hitmc.server;

import org.mpisws.hitmc.server.executor.BaseEventExecutor;

public class NamedEventExecutor extends BaseEventExecutor {

    private final StringBuilder stringBuilder;

    public NamedEventExecutor(final StringBuilder stringBuilder) {
        this.stringBuilder = stringBuilder;
    }

    public boolean execute(final NamedEvent namedEvent) {
        stringBuilder.append(namedEvent.getName());
        namedEvent.setExecuted();
        return true;
    }
}
