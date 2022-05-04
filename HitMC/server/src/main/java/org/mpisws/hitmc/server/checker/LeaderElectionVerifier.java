package org.mpisws.hitmc.server.checker;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.api.state.Vote;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class LeaderElectionVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionVerifier.class);

    private final TestingService testingService;
    private final Statistics statistics;

    public  LeaderElectionVerifier(final TestingService testingService, Statistics statistics) {
        this.testingService = testingService;
        this.statistics = statistics;
    }

    /***
     * Verify whether the result of the leader election achieves consensus
     * @return whether the result of the leader election achieves consensus
     */
    public boolean verify() {
        // There should be a unique leader; everyone else should be following or observing that leader
        int leader = -1;
        boolean consensus = true;
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            LOG.debug("--------------->Node Id: {}, NodeState: {}, " +
                            "leader: {},  isLeading: {}, " +
                            "isObservingOrFollowing:{}, {}, " +
                            "vote: {}",nodeId, testingService.getNodeStates().get(nodeId), leader,
                    isLeading(nodeId), isObservingOrFollowing(nodeId),
                    isObservingOrFollowing(nodeId, leader), testingService.getVotes().get(nodeId)
            );
        }
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            if (NodeState.OFFLINE.equals(testingService.getNodeStates().get(nodeId))) {
//                LOG.debug("NodeState.OFFLINE.equals(nodeStates.get(nodeId))");
                continue;
            }

            /**
             * There are four acceptable cases:
             *   1. leader == -1 && isLeading(nodeId) -- Fine, nodeId is the leader
             *   2. leader == -1 && isObservingOrFollowing(nodeId) -- Fine, whoever is in the final vote is the leader
             *   3. leader == nodeId && isLeading(nodeId) -- Fine, nodeId is the leader
             *   4. leader != -1 && isObservingOrFollowing(nodeId, leader) -- Fine, following the correct leader
             * In all other cases node is either still looking, or is another leader, or is following the wrong leader
             */
            if (leader == -1 && isLeading(nodeId)) {
//                LOG.debug("leader == -1 && isLeading(nodeId)");
                leader = nodeId;
            }
            else if (leader == -1 && isObservingOrFollowing(nodeId)) {
                final Vote vote = testingService.getVotes().get(nodeId);
                if (vote == null) {
                    consensus = false;
                    break;
                }
                leader = (int) vote.getLeader();
            }
            else if (!((leader == nodeId && isLeading(nodeId)) ||
                    (leader != -1 && isObservingOrFollowing(nodeId, leader)))) {
//                LOG.debug("------->else");
                consensus = false;
                break;
            }
        }
        if (leader != -1 && consensus) {
            LOG.debug("SUC");
            statistics.reportResult("SUCCESS");
            return true;
        }
        else {
            LOG.debug("FAIL");
            statistics.reportResult("FAILURE");
            return false;
        }
    }

    private boolean isLeading(final int nodeId) {
        final LeaderElectionState state = testingService.getLeaderElectionStates().get(nodeId);
        final Vote vote = testingService.getVotes().get(nodeId);
        // Node's state is LEADING and it has itself as the leader in the final vote
        return LeaderElectionState.LEADING.equals(state)
                && vote != null && nodeId == (int) vote.getLeader();
    }

    private boolean isObservingOrFollowing(final int nodeId, final int leader) {
        final Vote vote = testingService.getVotes().get(nodeId);
        // Node's state is FOLLOWING or OBSERVING and it has leader as the leader in the final vote
        return isObservingOrFollowing(nodeId) && vote != null && leader == (int) vote.getLeader();
    }

    private boolean isObservingOrFollowing(final int nodeId) {
        final LeaderElectionState state = testingService.getLeaderElectionStates().get(nodeId);
        return (LeaderElectionState.FOLLOWING.equals(state) || LeaderElectionState.OBSERVING.equals(state));
    }

    private boolean isLooking(final int nodeId) {
        return LeaderElectionState.LOOKING.equals(testingService.getLeaderElectionStates().get(nodeId));
    }

}
