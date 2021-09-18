package org.mpisws.hitmc.server;

public class NamedEventExecutor extends BaseEventExecutor {

    private final StringBuilder stringBuilder;

    public NamedEventExecutor(final StringBuilder stringBuilder) {
        super(null);
        this.stringBuilder = stringBuilder;
    }

    public boolean execute(final NamedEvent namedEvent) {
        stringBuilder.append(namedEvent.getName());
        namedEvent.setExecuted();
        return true;
    }
}
