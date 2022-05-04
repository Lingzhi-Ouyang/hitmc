package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.NodeCrashEvent;
import org.mpisws.hitmc.server.event.PartitionStartEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class PartitionStartExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionStopExecutor.class);
    private final TestingService testingService;

    //TODO: + partitionBudget
    private int partitionBudget = 10;

    public PartitionStartExecutor(final TestingService testingService, final FileWriter executionWriter) {
        super(executionWriter);
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final PartitionStartEvent event) throws IOException {
        boolean truelyExecuted = false;
        if (enablePartition()) {
            getExecutionWriter().write(event.toString() + '\n');
            getExecutionWriter().flush();
            testingService.startPartition(event.getNode1(), event.getNode2());
            testingService.waitAllNodesSteady();
            partitionBudget--;
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean enablePartition() {
        return partitionBudget > 0;
    }
}
