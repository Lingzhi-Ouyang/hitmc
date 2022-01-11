package org.apache.zookeeper.server.quorum;

import org.mpisws.hitmc.api.SchedulerRemote;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public aspect FastLeaderElectionAspect {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElectionAspect.class);

    private final SchedulerRemote scheduler;

    private int myId;

    private Integer lastSentMessageId = null;

    private FastLeaderElection.Notification notification;

    private int fleSubnodeId;
    private boolean fleSubnodeRegistered = false;
    private boolean workerReceiverSubnodeRegistered = false;
    private final Object nodeOnlineMonitor = new Object();

    private boolean fleSending = false;
    private boolean workerReceiverSending = false;

    public FastLeaderElectionAspect() {
        try {
            final Registry registry = LocateRegistry.getRegistry(2599);
            scheduler = (SchedulerRemote) registry.lookup(SchedulerRemote.REMOTE_NAME);
            LOG.debug("Found the remote HitMC scheduler.");
        } catch (final RemoteException e) {
            LOG.error("Couldn't locate the RMI registry.", e);
            throw new RuntimeException(e);
        } catch (final NotBoundException e) {
            LOG.error("Couldn't bind the HitMC scheduler.", e);
            throw new RuntimeException(e);
        }
    }

    public int getMyId() {
        return myId;
    }

    public SchedulerRemote getScheduler() {
        return scheduler;
    }

    // Identify the ID of this node

    pointcut setMyId(long id): set(long QuorumPeer.myid) && args(id);

    after(final long id): setMyId(id) {
        myId = (int) id;
        LOG.debug("Set myId = {}", myId);
    }

    // Intercept FastLeaderElection.lookForLeader()

    pointcut lookForLeader(): execution(* FastLeaderElection.lookForLeader());

    before(): lookForLeader() {
        try {
            LOG.debug("Registering FLE subnode");
            fleSubnodeId = scheduler.registerSubnode(myId, false);
            LOG.debug("Registered FLE subnode: id = {}", fleSubnodeId);
            synchronized (nodeOnlineMonitor) {
                fleSubnodeRegistered = true;
                if (workerReceiverSubnodeRegistered) {
                    scheduler.nodeOnline(myId);
                }
            }
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    after() returning (final Vote vote): lookForLeader() {
        try {
            scheduler.updateVote(myId, constructVote(vote));
            LOG.debug("Deregistring FLE subnode");
            scheduler.deregisterSubnode(fleSubnodeId);
            LOG.debug("-------------------Deregistered FLE subnode\n-------------\n");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Node state management

    public int registerWorkerReceiverSubnode() {
        final int workerReceiverSubnodeId;
        try {
            LOG.debug("Registring WorkerReceiver subnode");
            workerReceiverSubnodeId = scheduler.registerSubnode(myId, true);
            LOG.debug("Registered WorkerReceiver subnode: id = {}", workerReceiverSubnodeId);
            synchronized (nodeOnlineMonitor) {
                workerReceiverSubnodeRegistered = true;
                if (fleSubnodeRegistered) {
                    scheduler.nodeOnline(myId);
                }
            }
            return workerReceiverSubnodeId;
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterWorkerReceiverSubnode(final int workerReceiverSubnodeId) {
        try {
            LOG.debug("Deregistring WorkerReceiver subnode");
            scheduler.deregisterSubnode(workerReceiverSubnodeId);
            LOG.debug("Deregistered WorkerReceiver subnode");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public int getFleSubnodeId() {
        return fleSubnodeId;
    }

    public void setWorkerReceiverSending() {
        synchronized (nodeOnlineMonitor) {
            workerReceiverSending = true;
        }
    }

    public void workerReceiverPostSend(final int workerReceiverSubnodeId, final int msgId) throws RemoteException {
        synchronized (nodeOnlineMonitor) {
            workerReceiverSending = false;
            if (msgId == -1) {
                if (!fleSending) {
                    scheduler.nodeOffline(myId);
                }
                awaitShutdown(workerReceiverSubnodeId);
            }
        }
    }

    // Intercept message offering within FastLeaderElection, but not within WorkerReceiver

    pointcut offerWithinFastLeaderElection(Object object):
            within(FastLeaderElection) && !within(FastLeaderElection.Messenger.WorkerReceiver)
            && call(* LinkedBlockingQueue.offer(Object))
            && if (object instanceof FastLeaderElection.ToSend)
            && args(object);

    before(final Object object): offerWithinFastLeaderElection(object) {
        final FastLeaderElection.ToSend toSend = (FastLeaderElection.ToSend) object;

        final Set<Integer> predecessorIds = new HashSet<>();
        if (null != notification) {
            predecessorIds.add(notification.getMessageId());
        }
        if (null != lastSentMessageId) {
            predecessorIds.add(lastSentMessageId);
        }

        try {
            LOG.debug("FLE subnode {} is offering a message with predecessors {}", fleSubnodeId, predecessorIds.toString());
            synchronized (nodeOnlineMonitor) {
                fleSending = true;
            }
            final String payload = constructPayload(toSend);
            lastSentMessageId = scheduler.offerMessage(fleSubnodeId, (int) toSend.sid, predecessorIds, payload);
            LOG.debug("Scheduler returned id = {}", lastSentMessageId);
            synchronized (nodeOnlineMonitor) {
                fleSending = false;
                if (lastSentMessageId == -1) {
                    if (!workerReceiverSending) {
                        scheduler.nodeOffline(myId);
                    }
                    awaitShutdown(fleSubnodeId);
                }
            }
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (final Exception e) {
            LOG.debug("Uncaught exception", e);
        }
    }

    public void awaitShutdown(final int subnodeId) {
        try {
            LOG.debug("Deregistring subnode {}", subnodeId);
            scheduler.deregisterSubnode(subnodeId);
            // Going permanently to the wait queue
            nodeOnlineMonitor.wait();
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (final InterruptedException e) {
            LOG.debug("Interrupted from waiting on nodeOnlineMonitor", e);
        }

    }

    // Intercept polling the FastLeaderElection.recvqueue

    pointcut pollRecvQueue(LinkedBlockingQueue queue):
            withincode(* FastLeaderElection.lookForLeader())
            && call(* LinkedBlockingQueue.poll(..))
            && target(queue);

    before(final LinkedBlockingQueue queue): pollRecvQueue(queue) {
        if (queue.isEmpty()) {
            LOG.debug("My FLE.recvqueue is empty, go to RECEIVING state");
            // Going to block here
            try {
                scheduler.setReceivingState(fleSubnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    after(final LinkedBlockingQueue queue) returning (final FastLeaderElection.Notification notification): pollRecvQueue(queue) {
        this.notification = notification;
        LOG.debug("Received a notification with id = {}", notification.getMessageId());
    }

//    // Intercept state update in the election (within FastLeaderElection)
//
//    pointcut setPeerState(QuorumPeer.ServerState state):
//            within(FastLeaderElection)
//            && call(* QuorumPeer.setPeerState(QuorumPeer.ServerState))
//            && args(state);
//
//    after(final QuorumPeer.ServerState state) returning: setPeerState(state) {
//        final LeaderElectionState leState;
//        switch (state) {
//            case LEADING:
//                leState = LeaderElectionState.LEADING;
//                break;
//            case FOLLOWING:
//                leState = LeaderElectionState.FOLLOWING;
//                break;
//            case OBSERVING:
//                leState = LeaderElectionState.OBSERVING;
//                break;
//            case LOOKING:
//            default:
//                leState = LeaderElectionState.LOOKING;
//                break;
//        }
//        try {
//            LOG.debug("Node {} state: {}", myId, state);
//            scheduler.updateLeaderElectionState(myId, leState);
//            if(leState == LeaderElectionState.LOOKING){
//                scheduler.updateVote(myId, null);
//            }
//        } catch (final RemoteException e) {
//            LOG.error("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }

    // Intercept state update (within FastLeaderElection && QuorumPeer)

    pointcut setPeerState2(QuorumPeer.ServerState state):
                    call(* QuorumPeer.setPeerState(QuorumPeer.ServerState))
                    && args(state);

    after(final QuorumPeer.ServerState state) returning: setPeerState2(state) {
        final LeaderElectionState leState;
        switch (state) {
            case LEADING:
                leState = LeaderElectionState.LEADING;
                break;
            case FOLLOWING:
                leState = LeaderElectionState.FOLLOWING;
                break;
            case OBSERVING:
                leState = LeaderElectionState.OBSERVING;
                break;
            case LOOKING:
            default:
                leState = LeaderElectionState.LOOKING;
                break;
        }
        try {
            LOG.debug("Node {} state: {}", myId, state);
            scheduler.updateLeaderElectionState(myId, leState);
            if(leState == LeaderElectionState.LOOKING){
                scheduler.updateVote(myId, null);
            }
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

//    // this pointcut will include routine heartbeat settings of the role states
//
//    pointcut setPeerState3(QuorumPeer.ServerState s):
//            set(QuorumPeer.ServerState state) && args(s);
//
//    after(final QuorumPeer.ServerState s): setPeerState3(s) {
//        final LeaderElectionState leState;
//        switch (s) {
//            case LEADING:
//                leState = LeaderElectionState.LEADING;
//                break;
//            case FOLLOWING:
//                leState = LeaderElectionState.FOLLOWING;
//                break;
//            case OBSERVING:
//                leState = LeaderElectionState.OBSERVING;
//                break;
//            case LOOKING:
//            default:
//                leState = LeaderElectionState.LOOKING;
//                break;
//        }
//        try {
//            LOG.debug("Node {} state: {}", myId, s);
//            scheduler.updateLeaderElectionState(myId, leState);
//            if(leState == LeaderElectionState.LOOKING){
//                scheduler.updateVote(myId, null);
//            }
//        } catch (final RemoteException e) {
//            LOG.error("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }

    public String constructPayload(final FastLeaderElection.ToSend toSend) {
        return "from=" + myId +
                ", to=" + toSend.sid +
                ", leader=" + toSend.leader +
                ", state=" + toSend.state +
                ", zxid=" + toSend.zxid +
                ", electionEpoch=" + toSend.electionEpoch +
                ", peerEpoch=" + toSend.peerEpoch;
    }

    private org.mpisws.hitmc.api.state.Vote constructVote(final Vote vote) {
        return new org.mpisws.hitmc.api.state.Vote(vote.getId(), vote.getZxid(), vote.getElectionEpoch(), vote.getPeerEpoch());
    }
}