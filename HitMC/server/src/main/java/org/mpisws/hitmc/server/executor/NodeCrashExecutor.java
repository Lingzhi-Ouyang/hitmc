package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.NodeCrashEvent;
import org.mpisws.hitmc.server.event.NodeStartEvent;

import java.io.FileWriter;
import java.io.IOException;

public class NodeCrashExecutor extends BaseEventExecutor {

    private final TestingService testingService;

    private int crashBudget;

    public NodeCrashExecutor(final TestingService testingService, final FileWriter executionWriter, final int crashBudget) {
        super(executionWriter);
        this.testingService = testingService;
        this.crashBudget = crashBudget;
    }

    @Override
    public boolean execute(final NodeCrashEvent event) throws IOException {
        boolean truelyExecuted = false;
        if (hasCrashes() || event.hasLabel()) {
            final int nodeId = event.getNodeId();
            if (!event.hasLabel()) {
                decrementCrashes();
            }
            if (testingService.getNodeStartExecutor().hasReboots()) {
                final NodeStartEvent nodeStartEvent = new NodeStartEvent(testingService.generateEventId(), nodeId, testingService.getNodeStartExecutor());
                nodeStartEvent.addDirectPredecessor(event);
                testingService.addEvent(nodeStartEvent);
            }
            getExecutionWriter().write(event.toString() + '\n');
            getExecutionWriter().flush();
            testingService.stopNode(nodeId);
            testingService.waitAllNodesSteady();
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean hasCrashes() {
        return crashBudget > 0;
    }

    public void decrementCrashes() {
        crashBudget--;
    }
}
