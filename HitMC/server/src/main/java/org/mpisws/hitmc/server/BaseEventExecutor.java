package org.mpisws.hitmc.server;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.FileWriter;
import java.io.IOException;

public class BaseEventExecutor {

    private final FileWriter executionWriter;

    public BaseEventExecutor(final FileWriter executionWriter) {
        this.executionWriter = executionWriter;
    }

    public FileWriter getExecutionWriter() {
        return executionWriter;
    }

    public boolean execute(final NodeCrashEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final NodeStartEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final MessageEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final ClientRequestEvent event) throws IOException {
        throw new NotImplementedException();
    }
}
