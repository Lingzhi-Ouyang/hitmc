package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class MessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MessageExecutor.class);

    private final TestingService testingService;

    public  MessageExecutor(final TestingService testingService, final FileWriter executionWriter) {
        super(executionWriter);
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final MessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        getExecutionWriter().write(event.toString() + '\n');
        getExecutionWriter().flush();
        testingService.releaseMessage(event);
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Message executed: {}", event.toString());
        return true;
    }
}
