package org.mpisws.hitmc.server;

import org.mpisws.hitmc.api.*;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.api.state.Vote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Scheduler implements SchedulerRemote {

    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

    @Autowired
    private SchedulerConfiguration schedulerConfiguration;

    @Autowired
    private Ensemble ensemble;

    private SchedulingStrategy schedulingStrategy;
    private MessageExecutor messageExecutor;
    private NodeStartExecutor nodeStartExecutor;
    private NodeCrashExecutor nodeCrashExecutor;

    private Statistics statistics;
    private FileWriter statisticsWriter;
    private FileWriter executionWriter;
    //private FileWriter vectorClockWriter;

    private final Object controlMonitor = new Object();

    private final List<NodeState> nodeStates = new ArrayList<>();
    private final List<Subnode> subnodes = new ArrayList<>();
    private final List<Set<Subnode>> subnodeSets = new ArrayList<>();

    private final AtomicInteger eventIdGenerator = new AtomicInteger();

    private final Map<Integer, MessageEvent> messageEventMap = new HashMap<>();
    private final List<NodeStartEvent> lastNodeStartEvents = new ArrayList<>();
    private final List<Boolean> firstMessage = new ArrayList<>();

    private int messageInFlight;

    private final List<Vote> votes = new ArrayList<>();
    private final List<LeaderElectionState> leaderElectionStates = new ArrayList<>();

    //private int[][] vectorClock;
    //private Map <Event, List> vectorClockEvent;
    private Map <Integer, Integer> getSubNodeByID;


    public void loadConfig(final String[] args) throws SchedulerConfigurationException {
        schedulerConfiguration.load(args);
    }

    private void configureNextExecution(final int executionId) throws SchedulerConfigurationException, IOException {
        ensemble.configureEnsemble(executionId);

        // Configure scheduling strategy
        final Random random = new Random();
        final long seed;
        if (schedulerConfiguration.hasRandomSeed()) {
            seed = schedulerConfiguration.getRandomSeed() + executionId * executionId;
        }
        else {
            seed = random.nextLong();
        }
        random.setSeed(seed);

        if ("pctcp".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final PCTCPStatistics pctcpStatistics = new PCTCPStatistics();
            statistics = pctcpStatistics;

            LOG.debug("Configuring PCTCPStrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            pctcpStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            pctcpStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new PCTCPStrategy(schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, pctcpStatistics);
        }
        else if ("tapct".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final TAPCTstatistics tapctStatistics = new TAPCTstatistics();
            statistics = tapctStatistics;

            LOG.debug("Configuring taPCTstrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            tapctStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            tapctStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new TAPCTstrategy(
                    schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, tapctStatistics);
        }
        else if ("Poffline".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final PofflineStatistics pOfflineStatistics = new PofflineStatistics();
            statistics = pOfflineStatistics;

            LOG.debug("Configuring PofflineStrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            pOfflineStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            pOfflineStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new PofflineStrategy(schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, pOfflineStatistics);
        }
        else if("pos".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final POSstatistics posStatistics = new POSstatistics();
            statistics = posStatistics;

            LOG.debug("Configuring POSstrategy: randomSeed={}", seed);
            schedulingStrategy = new POSstrategy(schedulerConfiguration.getMaxEvents(), random, posStatistics);
        }

        else if("posd".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final POSdStatistics posdStatistics = new POSdStatistics();
            statistics = posdStatistics;

            posdStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            LOG.debug("Configuring POSdStrategy: randomSeed={}", seed);
            schedulingStrategy = new POSdStrategy(schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), random, posdStatistics);
        }
        else if("rapos".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final RAPOSstatistics raposStatistics = new RAPOSstatistics();
            statistics = raposStatistics;

            LOG.debug("Configuring RAPOSstrategy: randomSeed={}", seed);
            schedulingStrategy = new RAPOSstrategy(random, raposStatistics);
        }
        else {
            final RandomWalkStatistics randomWalkStatistics = new RandomWalkStatistics();
            statistics = randomWalkStatistics;

            LOG.debug("Configuring RandomWalkStrategy: randomSeed={}", seed);
            schedulingStrategy = new RandomWalkStrategy(random, randomWalkStatistics);
        }
        statistics.reportRandomSeed(seed);

        executionWriter = new FileWriter(executionId + File.separator + schedulerConfiguration.getExecutionFile());
        statisticsWriter = new FileWriter(executionId + File.separator + schedulerConfiguration.getStatisticsFile());
        //vectorClockWriter = new FileWriter(executionId + File.separator + "vectorClock"); //simin

        // Configure executors
        messageExecutor = new MessageExecutor(this, executionWriter);
        nodeStartExecutor = new NodeStartExecutor(this, executionWriter, schedulerConfiguration.getNumReboots());
        nodeCrashExecutor = new NodeCrashExecutor(this, executionWriter, schedulerConfiguration.getNumCrashes());

        // Configure nodes and subnodes
        nodeStates.clear();
        subnodeSets.clear();
        subnodes.clear();
        for (int i = 0 ; i < schedulerConfiguration.getNumNodes(); i++) {
            nodeStates.add(NodeState.STARTING);
            subnodeSets.add(new HashSet<Subnode>());
        }
        //vectorClock = new int[][]{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
        //vectorClockEvent = new HashMap<>();
        getSubNodeByID = new HashMap<>();

        eventIdGenerator.set(0);
        messageEventMap.clear();
        messageInFlight = 0;

        firstMessage.clear();
        firstMessage.addAll(Collections.<Boolean>nCopies(schedulerConfiguration.getNumNodes(), null));

        votes.clear();
        votes.addAll(Collections.<Vote>nCopies(schedulerConfiguration.getNumNodes(), null));

        leaderElectionStates.clear();
        leaderElectionStates.addAll(Collections.nCopies(schedulerConfiguration.getNumNodes(), LeaderElectionState.LOOKING));

        // Configure lastNodeStartEvents
        lastNodeStartEvents.clear();
        lastNodeStartEvents.addAll(Collections.<NodeStartEvent>nCopies(schedulerConfiguration.getNumNodes(), null));

        // Generate node crash events
        if (schedulerConfiguration.getNumCrashes() > 0) {
            for (int i = 0; i < schedulerConfiguration.getNumNodes(); i++) {
                final NodeCrashEvent nodeCrashEvent = new NodeCrashEvent(generateEventId(), i, nodeCrashExecutor);
                schedulingStrategy.add(nodeCrashEvent);
            }
        }
    }

    public void start() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the scheduler");
        initRemote();
        for (int executionId = 1; executionId <= schedulerConfiguration.getNumExecutions(); ++executionId) {
            configureNextExecution(executionId);
            statistics.startTimer();
            ensemble.startEnsemble();
            int totalExecuted = 0;
            synchronized (controlMonitor) {
                waitAllNodesSteady();
                LOG.debug("All Nodes steady");
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    LOG.debug("Step: {}", totalExecuted);
                    final Event event = schedulingStrategy.nextEvent();
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        LOG.debug("executed event: {}", event.toString());
                        /*
                        int currNode;
                        try {
                            vectorClockWriter.write("Executed " + event.toString() + "\n");
                        }
                        catch (final IOException e) {
                            LOG.debug("IO exception", e);
                        }
                        if (event instanceof MessageEvent) {
                            currNode = ((MessageEvent)event).getReceivingNodeId();
                            vectorClock[currNode][currNode]++;
                            List <Integer> tmp = vectorClockEvent.get(event);
                            vectorClock[currNode][0] = Math.max(vectorClock[currNode][0], tmp.get(0));
                            vectorClock[currNode][1] = Math.max(vectorClock[currNode][1], tmp.get(1));
                            vectorClock[currNode][2] = Math.max(vectorClock[currNode][2], tmp.get(2));
                            vectorClockEvent.remove(event);
                        }
                        else if (event instanceof NodeCrashEvent){
                            currNode = ((NodeCrashEvent)event).getNodeId();
                            vectorClock[currNode][currNode]++;
                        }
                        else if (event instanceof NodeStartEvent){
                            currNode = ((NodeStartEvent)event).getNodeId();
                            vectorClock[currNode][currNode]++;
                        }
                        else{
                            LOG.debug("- - - - - - - - - - - - - - - - - - - - - -");
                            continue;
                        }
                        try{
                            vectorClockWriter.write(vectorClock[currNode][0] + " " + vectorClock[currNode][1] + " " + vectorClock[currNode][2] + "\n");
                        }
                        catch (final IOException e) {
                            LOG.debug("IO exception", e);
                        }*/
                        ++totalExecuted;
                        verifyConsensus();
                    }
                }
                waitAllNodesDone();
            }
//            ensemble.stopEnsemble();
            statistics.endTimer();
            statistics.reportTotalExecutedEvents(totalExecuted);
            verifyConsensus();
            statisticsWriter.write(statistics.toString() + '\n');
            LOG.info(statistics.toString());
            executionWriter.close();
            statisticsWriter.close();
            //vectorClockWriter.close();
        }
    }

    private void  initRemote() {
        try {
            final SchedulerRemote schedulerRemote = (SchedulerRemote) UnicastRemoteObject.exportObject(this, 0);
            final Registry registry = LocateRegistry.createRegistry(2599);
//            final Registry registry = LocateRegistry.getRegistry(2599);
            LOG.debug("{}, {}", SchedulerRemote.REMOTE_NAME, schedulerRemote);
            registry.rebind(SchedulerRemote.REMOTE_NAME, schedulerRemote);
            LOG.debug("Bound the remote scheduler");
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception while initializing the scheduler.", e);
            throw new RuntimeException(e);
        }
    }

    public Ensemble getEnsemble() {
        return ensemble;
    }

    public NodeStartExecutor getNodeStartExecutor() {
        return nodeStartExecutor;
    }

    public NodeCrashExecutor getNodeCrashExecutor() {
        return nodeCrashExecutor;
    }

    public void addEvent(final Event event) {
        schedulingStrategy.add(event);

        /*try {
            vectorClockWriter.write("Added " + event.toString() + "\n");
        }
        catch (final IOException e) {
            LOG.debug("IO exception", e);
        }
        if (event instanceof MessageEvent)
        {
            LOG.debug(getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId()) + " " + ((MessageEvent) event).getSendingSubnodeId());
            vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())]++;
            List <Integer> tmp = new ArrayList<Integer>();
            tmp.add(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][0]);
            tmp.add(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][1]);
            tmp.add(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][2]);
            vectorClockEvent.put(event, tmp);
            try {
                vectorClockWriter.write(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][0] + " " +
                        vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][1] + " " + vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][2] + "\n");
            }
            catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }*/
    }

    public void setLastNodeStartEvent(final int nodeId, final NodeStartEvent nodeStartEvent) {
        lastNodeStartEvents.set(nodeId, nodeStartEvent);
    }

    @Override
    public int offerMessage(final int sendingSubnodeId, final int receivingNodeId, final Set<Integer> predecessorMessageIds, final String payload) {
        final List<Event> predecessorEvents = new ArrayList<>();
        for (final int messageId : predecessorMessageIds) {
            predecessorEvents.add(messageEventMap.get(messageId));
        }
        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();
        getSubNodeByID.put(sendingSubnodeId, sendingNodeId);

        // We want to determinize the order in which the first messages are added, so we wait until
        // all nodes with smaller ids have offered their first message.
        synchronized (controlMonitor) {
            if (sendingNodeId > 0 && firstMessage.get(sendingNodeId - 1) == null) {
                waitFirstMessageOffered(sendingNodeId - 1);
            }
        }

        final NodeStartEvent lastNodeStartEvent = lastNodeStartEvents.get(sendingNodeId);
        if (null != lastNodeStartEvent) {
            predecessorEvents.add(lastNodeStartEvent);
        }

        int id = generateEventId();
        final MessageEvent messageEvent = new MessageEvent(id, sendingSubnodeId, receivingNodeId, payload, messageExecutor);
        messageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Node {} is offering a message: msgId = {}, predecessors = {}", sendingNodeId,
                    id, predecessorMessageIds.toString());
            messageEventMap.put(id, messageEvent);
            addEvent(messageEvent);
            if (firstMessage.get(sendingNodeId) == null) {
                firstMessage.set(sendingNodeId, true);
            }
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();
            waitMessageReleased(id, sendingNodeId);
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId))) {
                id = -1;
                messageEvent.setExecuted();
            }
        }

        return id;
    }

    @Override
    public int getMessageInFlight() {
        return messageInFlight;
    }

    // Should be called while holding a lock on controlMonitor
    public void releaseMessage(final MessageEvent event) {
        messageInFlight = event.getId();
        final Subnode sendingSubnode = subnodes.get(event.getSendingSubnodeId());
        sendingSubnode.setState(SubnodeState.PROCESSING);
        for (final Subnode subnode : subnodeSets.get(event.getReceivingNodeId())) {
            if (subnode.isMainReceiver() && SubnodeState.RECEIVING.equals(subnode.getState())) {
                subnode.setState(SubnodeState.PROCESSING);
            }
        }
        controlMonitor.notifyAll();
    }

    @Override
    public int registerSubnode(final int nodeId, final boolean mainReceiver) throws RemoteException {
        final int subnodeId;
        synchronized (controlMonitor) {
            subnodeId = subnodes.size();
            final Subnode subnode = new Subnode(subnodeId, nodeId, mainReceiver);
            subnodes.add(subnode);
            subnodeSets.get(nodeId).add(subnode);
        }
        return subnodeId;
    }

    @Override
    public void deregisterSubnode(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            subnodeSets.get(subnode.getNodeId()).remove(subnode);
            if (!SubnodeState.UNREGISTERED.equals(subnode.getState())) {
                subnode.setState(SubnodeState.UNREGISTERED);
                // All subnodes may have become steady; give the scheduler a chance to make progress
                controlMonitor.notifyAll();
            }
        }
    }

    @Override
    public void setProcessingState(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            if (SubnodeState.RECEIVING.equals(subnode.getState())) {
                subnode.setState(SubnodeState.PROCESSING);
            }
        }
    }

    @Override
    public void setReceivingState(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                subnode.setState(SubnodeState.RECEIVING);
                controlMonitor.notifyAll();
            }
        }
    }

    @Override
    public void nodeOnline(final int nodeId) throws RemoteException {
        synchronized (controlMonitor) {
            nodeStates.set(nodeId, NodeState.ONLINE);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void nodeOffline(final int nodeId) throws RemoteException {
        synchronized (controlMonitor) {
            nodeStates.set(nodeId, NodeState.OFFLINE);
            controlMonitor.notifyAll();
        }
    }

    // Should be called while holding a lock on controlMonitor
    public void waitAllNodesSteady() {
        wait(allNodesSteady, 0L);
    }

    // Should be called while holding a lock on controlMonitor
    private void waitAllNodesDone() {
        wait(allNodesDone, 1000L);
    }

    public void startNode(final int nodeId) {
        nodeStates.set(nodeId, NodeState.STARTING);
        ensemble.startNode(nodeId);
    }


    public void stopNode(final int nodeId) {
        boolean hasSending = false;
        for (final Subnode subnode : subnodeSets.get(nodeId)) {
            if (SubnodeState.SENDING.equals(subnode.getState())) {
                hasSending = true;
                break;
            }
        }
        if (hasSending) {
            nodeStates.set(nodeId, NodeState.STOPPING);
            controlMonitor.notifyAll();
            waitAllNodesSteady();
        }
        for (final Subnode subnode : subnodeSets.get(nodeId)) {
            subnode.setState(SubnodeState.UNREGISTERED);
        }
        subnodeSets.get(nodeId).clear();
        nodeStates.set(nodeId, NodeState.OFFLINE);
        ensemble.stopNode(nodeId);
        controlMonitor.notifyAll();
    }

    public int generateEventId() {
        return eventIdGenerator.incrementAndGet();
    }

    @Override
    public void updateVote(final int nodeId, final Vote vote) throws RemoteException {
        synchronized (controlMonitor) {
            votes.set(nodeId, vote);
            controlMonitor.notifyAll();
            try {
                executionWriter.write("Node " + nodeId + " final vote: " + vote.toString() + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    @Override
    public void updateLeaderElectionState(final int nodeId, final LeaderElectionState state) throws RemoteException {
        synchronized (controlMonitor) {
            leaderElectionStates.set(nodeId, state);
            try {
                executionWriter.write("Node " + nodeId + " state: " + state + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    private void verifyConsensus() {
        // There should be a unique leader; everyone else should be following or observing that leader
        int leader = -1;
        boolean consensus = true;
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
            LOG.debug("--------------->Node Id: {}, NodeState: {}, " +
                            "leader: {},  isLeading: {}, " +
                            "isObservingOrFollowing:{}, {}, " +
                            "vote: {}",nodeId, nodeStates.get(nodeId), leader,
                    isLeading(nodeId), isObservingOrFollowing(nodeId),
                    isObservingOrFollowing(nodeId, leader), votes.get(nodeId)
            );
        }
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
            if (NodeState.OFFLINE.equals(nodeStates.get(nodeId))) {
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
                final Vote vote = votes.get(nodeId);
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
        }
        else {
            LOG.debug("FAIL");
            statistics.reportResult("FAILURE");
        }
    }

    private boolean isLeading(final int nodeId) {
        final LeaderElectionState state = leaderElectionStates.get(nodeId);
        final Vote vote = votes.get(nodeId);
        // Node's state is LEADING and it has itself as the leader in the final vote
        return LeaderElectionState.LEADING.equals(state)
                && vote != null && nodeId == (int) vote.getLeader();
    }

    private boolean isObservingOrFollowing(final int nodeId, final int leader) {
        final Vote vote = votes.get(nodeId);
        // Node's state is FOLLOWING or OBSERVING and it has leader as the leader in the final vote
        return isObservingOrFollowing(nodeId) && vote != null && leader == (int) vote.getLeader();
    }

    private boolean isObservingOrFollowing(final int nodeId) {
        final LeaderElectionState state = leaderElectionStates.get(nodeId);
        return (LeaderElectionState.FOLLOWING.equals(state) || LeaderElectionState.OBSERVING.equals(state));
    }

    private boolean isLooking(final int nodeId) {
        return LeaderElectionState.LOOKING.equals(leaderElectionStates.get(nodeId));
    }

    private class AllNodesSteady implements WaitPredicate {

        @Override
        public boolean isTrue() {
            for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                final NodeState nodeState = nodeStates.get(nodeId);
                if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState)) {
                    return false;
                }
                for (final Subnode subnode : subnodeSets.get(nodeId)) {
                    if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                        return false;
                    }
                }
            }
            for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                final NodeState nodeState = nodeStates.get(nodeId);
                for (final Subnode subnode : subnodeSets.get(nodeId)) {
                    LOG.debug("Node {} status: {}, subnode {} status: {}, is main receiver : {}",
                            nodeId, nodeState, subnode.getId(), subnode.getState(), subnode.isMainReceiver());
                }
            }
            return true;
        }

        @Override
        public String describe() {
            return "allNodesSteady";
        }
    }

    private final WaitPredicate allNodesSteady = new AllNodesSteady();

    private class AllNodesDone implements WaitPredicate {

        @Override
        public boolean isTrue() {
            for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                if (!NodeState.OFFLINE.equals(nodeStates.get(nodeId))
                        && (!NodeState.ONLINE.equals(nodeStates.get(nodeId)) || votes.get(nodeId) == null)) {
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

    private final WaitPredicate allNodesDone = new AllNodesDone();

    private class FirstMessageOffered implements WaitPredicate {

        private final int nodeId;

        public FirstMessageOffered(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public boolean isTrue() {
            return firstMessage.get(nodeId) != null;
        }

        @Override
        public String describe() {
            return "first message from node " + nodeId;
        }
    }

    private void waitFirstMessageOffered(final int nodeId) {
        final WaitPredicate firstMessageOffered = new FirstMessageOffered(nodeId);
        wait(firstMessageOffered, 0L);
    }

    private class MessageReleased implements WaitPredicate {

        private final int msgId;
        private final int sendingNodeId;

        public MessageReleased(int msgId, int sendingNodeId) {
            this.msgId = msgId;
            this.sendingNodeId = sendingNodeId;
        }

        @Override
        public boolean isTrue() {
            return getMessageInFlight() == msgId || NodeState.STOPPING.equals(nodeStates.get(sendingNodeId));
        }

        @Override
        public String describe() {
            return "release of message " + msgId + " sent by node " + sendingNodeId;
        }
    }

    private void waitMessageReleased(final int msgId, final int sendingNodeId) {
        final WaitPredicate messageReleased = new MessageReleased(msgId, sendingNodeId);
        wait(messageReleased, 0L);
    }

    private void wait(final WaitPredicate predicate, final long timeout) {
        LOG.debug("Waiting for {}", predicate.describe());
        final long startTime = System.currentTimeMillis();
        long endTime = startTime;
        while (!predicate.isTrue() && (timeout == 0L || endTime - startTime < timeout)) {
            try {
                if (timeout == 0L) {
                    controlMonitor.wait();
                } else {
                    controlMonitor.wait(Math.max(1L, timeout - (endTime - startTime)));
                }
            } catch (final InterruptedException e) {
                LOG.debug("Interrupted from waiting on the control monitor");
            } finally {
                endTime = System.currentTimeMillis();
            }
        }
        LOG.debug("Done waiting for {}", predicate.describe());
    }
    public int nodIdOfSubNode (int subNodeID){
        return getSubNodeByID.get(subNodeID);
    }
}