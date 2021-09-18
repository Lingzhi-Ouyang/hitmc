package org.mpisws.hitmc.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class NodeStartExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStartExecutor.class);

    private final Scheduler scheduler;

    private int rebootBudget;

    public NodeStartExecutor(final Scheduler scheduler, final FileWriter executionWriter, final int rebootBudget) {
        super(executionWriter);
        this.scheduler = scheduler;
        this.rebootBudget = rebootBudget;
    }

    @Override
    public boolean execute(final NodeStartEvent event)  throws IOException {
        boolean truelyExecuted = false;
        if (hasReboots()) {
            final int nodeId = event.getNodeId();
            scheduler.setLastNodeStartEvent(nodeId, event);
            getExecutionWriter().write(event.toString() + '\n');
            getExecutionWriter().flush();
            scheduler.startNode(nodeId);
            scheduler.waitAllNodesSteady();
            rebootBudget--;
            if (scheduler.getNodeCrashExecutor().hasCrashes()) {
                final NodeCrashEvent nodeCrashEvent = new NodeCrashEvent(scheduler.generateEventId(), nodeId, scheduler.getNodeCrashExecutor());
                nodeCrashEvent.addDirectPredecessor(event);
                scheduler.addEvent(nodeCrashEvent);
            }
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean hasReboots() {
        return rebootBudget > 0;
    }
}
