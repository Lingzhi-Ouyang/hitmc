package org.mpisws.hitmc.server;

import org.apache.zookeeper.KeeperException;
import org.mpisws.hitmc.api.state.ClientRequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class ClientRequestExecutor extends BaseEventExecutor{
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestExecutor.class);

    private final Scheduler scheduler;

    public ClientRequestExecutor(final Scheduler scheduler, final FileWriter executionWriter) {
        super(executionWriter);
        this.scheduler = scheduler;
    }

    @Override
    public boolean execute(final ClientRequestEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed client request event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing client request event: {}", event.toString());
        try {
            scheduler.releaseClientRequest(event);
            final ClientRequestEvent clientRequestEvent = new ClientRequestEvent(scheduler.generateEventId(),
                    event.getType(),scheduler.getClientReadExecutor());
            scheduler.addEvent(clientRequestEvent);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        getExecutionWriter().write(event.toString() + '\n');
        getExecutionWriter().flush();
        event.setExecuted();
        LOG.debug("Client request executed: {}", event.toString());
        return true;
    }
}
