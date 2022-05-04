package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.PartitionStartEvent;
import org.mpisws.hitmc.server.event.PartitionStopEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class PartitionStopExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionStopExecutor.class);
    private final TestingService testingService;

    //TODO: + partitionBudget
    private int partitionStopBudget = 10;

    public PartitionStopExecutor(final TestingService testingService, final FileWriter executionWriter) {
        super(executionWriter);
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final PartitionStopEvent event) throws IOException {
        boolean truelyExecuted = false;
        if (enablePartitionStop()) {
            getExecutionWriter().write(event.toString() + '\n');
            getExecutionWriter().flush();
            testingService.stopPartition(event.getNode1(), event.getNode2());
            testingService.waitAllNodesSteady();
            partitionStopBudget--;
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean enablePartitionStop() {
        return partitionStopBudget > 0;
    }
}
