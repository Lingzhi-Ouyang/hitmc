package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/***
 * Wait Predicate for the end of an execution.
 */
public class AllNodesDone implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesDone.class);

    private final TestingService testingService;

    public AllNodesDone(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            LOG.debug("nodeId: {}, state: {}, votes: {}", nodeId, testingService.getNodeStates().get(nodeId), testingService.getVotes().get(nodeId));
            if (!NodeState.OFFLINE.equals(testingService.getNodeStates().get(nodeId))
                    && (!NodeState.ONLINE.equals(testingService.getNodeStates().get(nodeId)) || testingService.getVotes().get(nodeId) == null)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "allNodesDone";
    }
}
