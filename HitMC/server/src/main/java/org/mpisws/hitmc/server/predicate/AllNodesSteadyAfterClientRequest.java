package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for client request event when
 * both learnerHandlerSender and syncProcessor are intercepted
 * - leader: SYNC_PROCESSOR in SENDING state && LEARNER_HANDLER in SENDING states, other subnodes in SENDING / RECEIVING states
 * - follower: all subnodes in SENDING / RECEIVING states
 */
public class AllNodesSteadyAfterClientRequest implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyAfterClientRequest.class);

    private final TestingService testingService;

    public AllNodesSteadyAfterClientRequest(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = testingService.getNodeStates().get(nodeId);
            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState)) {
                LOG.debug("------not yet Steady-----Node {} status: {}",
                        nodeId, nodeState);
                return false;
            } else {
                LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
            }
            LeaderElectionState leaderElectionState = testingService.getLeaderElectionStates().get(nodeId);
            if (LeaderElectionState.LEADING.equals(leaderElectionState)) {
                if (!leaderSteadyAfterClientRequest(nodeId)) {
                    return false;
                }
            } else if (LeaderElectionState.FOLLOWING.equals(leaderElectionState)) {
                if (!followerSteadyAfterClientRequest(nodeId)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "allNodesSteadyAfterClientRequest";
    }

    private boolean leaderSteadyAfterClientRequest(final int nodeId) {
        for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType()) ||
                    SubnodeType.LEARNER_HANDLER_SENDER.equals(subnode.getSubnodeType())) {
                if (!SubnodeState.SENDING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for leader's {} thread-----" +
                                    "Node {} subnode {} status: {}",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            } else if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady for leader's {} thread-----" +
                                "Node {} subnode {} status: {}",
                        subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                return false;
            }

            LOG.debug("-----------Leader node {} subnode {} status: {}, subnode type: {}",
                        nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
        return true;
    }

    private boolean followerSteadyAfterClientRequest(final int nodeId) {
        for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady for follower's {} thread-----" +
                                "Node {} subnode {} status: {}",
                        subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                return false;
            }
            LOG.debug("-----------Follower node {} subnode {} status: {}, subnode type: {}",
                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
        return true;
    }
}
