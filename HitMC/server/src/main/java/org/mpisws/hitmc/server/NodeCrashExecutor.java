package org.mpisws.hitmc.server;

import java.io.FileWriter;
import java.io.IOException;

public class NodeCrashExecutor extends BaseEventExecutor {

    private final Scheduler scheduler;

    private int crashBudget;

    public NodeCrashExecutor(final Scheduler scheduler, final FileWriter executionWriter, final int crashBudget) {
        super(executionWriter);
        this.scheduler = scheduler;
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
            if (scheduler.getNodeStartExecutor().hasReboots()) {
                final NodeStartEvent nodeStartEvent = new NodeStartEvent(scheduler.generateEventId(), nodeId, scheduler.getNodeStartExecutor());
                nodeStartEvent.addDirectPredecessor(event);
                scheduler.addEvent(nodeStartEvent);
            }
            getExecutionWriter().write(event.toString() + '\n');
            getExecutionWriter().flush();
            scheduler.stopNode(nodeId);
            scheduler.waitAllNodesSteady();
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
