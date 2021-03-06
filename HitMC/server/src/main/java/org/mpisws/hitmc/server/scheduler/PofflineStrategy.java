package org.mpisws.hitmc.server.scheduler;

import org.mpisws.hitmc.server.event.Event;
import org.mpisws.hitmc.server.event.DummyEvent;
import org.mpisws.hitmc.server.event.MessageEvent;
import org.mpisws.hitmc.server.event.NodeCrashEvent;
import org.mpisws.hitmc.server.statistics.PofflineStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PofflineStrategy implements SchedulingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PofflineStrategy.class);

    private final PofflineRandom random;
    private final Map<Integer, Integer> priorityChangePoints;
    private final List<Chain> chains;
    private final List<List<Chain>> chainPartition = new ArrayList<>();

    private final DummyEvent dummyEvent = new DummyEvent();
    private final int maxEvents;
    private int eventsAdded = 0;
    private int eventsPrepared = 0;
    private int numPriorityChangePoints;
    private int numRacy = 0;

    private final PofflineStatistics statistics;

    public PofflineStrategy(int maxEvents, int numPriorityChangePoints, final Random random, final PofflineStatistics statistics) {
        this.maxEvents = maxEvents;
        this.random = new PofflineRandom(maxEvents, numPriorityChangePoints, random);
        this.chains = new ArrayList<>(Collections.<Chain>nCopies(numPriorityChangePoints, null));
        this.priorityChangePoints = this.random.generatePriorityChangePoints();
        this.statistics = statistics;
        this.statistics.reportPriorityChangePoints(priorityChangePoints);
        this.numPriorityChangePoints = numPriorityChangePoints;
    }

    private boolean nextEventPrepared = false;
    private Event nextEvent;

    @Override
    public boolean hasNextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        LOG.debug("hasNextEvent == {}", nextEvent != null);
        return nextEvent != null;
    }

    @Override
    public Event nextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        nextEventPrepared = false;
        return nextEvent;
    }

    private void prepareNextEvent() {
        LOG.debug("Preparing next event...");
        if (eventsPrepared == maxEvents) {
            dummyEvent.execute();
        }
        nextEvent = null;
        int totalEnabled = 0;
        for (int i = chains.size() - 1; i >= 0; i--) {
            final Chain chain = chains.get(i);
            if (chain == null || chain.isEmpty()) {
                continue;
            }
            final Event event = chain.peekFirst();
            if (event.isEnabled()) {
                totalEnabled++;
                if (!nextEventPrepared) {
                    nextEvent = event;
                    chain.removeFirst();
                    updatePriority(nextEvent, i);
                    eventsPrepared++;
                    nextEventPrepared = true;
                }
            }
        }
        nextEventPrepared = true;
        statistics.reportNumberOfChains(chains.size());
        statistics.reportNumberOfEnabledEvents(totalEnabled);
    }

    @Override
    public void add(final Event event) {
        eventsAdded++;
        if (eventsAdded > maxEvents) {
            // Make sure the event stays disabled until the first maxEvents events are dispatched
            event.addDirectPredecessor(dummyEvent);
        }
        LOG.debug("Adding event: {}", event.toString());

        for (int i = 0; i < chainPartition.size(); i++) {
            final List<Chain> block = chainPartition.get(i);

            final Iterator<Chain> blockIterator = block.iterator();
            while (blockIterator.hasNext()) {
                final Chain chain = blockIterator.next();
                if (chain.peekLast().happensBefore(event)) {
                    chain.addLast(event);
                    if (i > 0) {
                        blockIterator.remove();
                        chainPartition.get(i - 1).add(chain);
                        Collections.swap(chainPartition, i - 1, i);
                    }
                    return;
                }
            }

            if (block.size() <= i) {
                block.add(createChain(event));
                return;
            }
        }

        final List<Chain> block = new LinkedList<>();
        block.add(createChain(event));
        chainPartition.add(block);

        // Make sure prepareNextEvent() is triggered
        if (nextEventPrepared && nextEvent == null) {
            nextEventPrepared = false;
        }
    }

    private Chain createChain(final Event event) {
        final Chain chain = new Chain(event);
        final int chainPosition = random.generateChainPosition(chains.size());
        chains.add(chainPosition, chain);
        return chain;
    }

    private final class Chain {

        private final Deque<Event> deque;

        private Event lastTail;

        public Chain(final Event event) {
            deque = new ArrayDeque<>();
            addLast(event);
        }

        public boolean isEmpty() {
            return deque.isEmpty();
        }

        public Event peekFirst() {
            return deque.peekFirst();
        }

        public Event peekLast() {
            return lastTail;
        }

        public Event removeFirst() {
            return deque.removeFirst();
        }

        public void addLast(final Event event) {
            deque.addLast(event);
            lastTail = event;
        }
    }
    private boolean isRacy (Event e1, Event e2){
        if (e1 instanceof MessageEvent && e2 instanceof MessageEvent) {
            if (((MessageEvent) e1).getReceivingNodeId() == ((MessageEvent) e2).getReceivingNodeId()) {
                return true;
            }
        }
        if (e1 instanceof MessageEvent && e2 instanceof NodeCrashEvent) {
            if (((MessageEvent) e1).getReceivingNodeId() == ((NodeCrashEvent) e2).getNodeId()) {
                return true;
            }
        }
        return false;
    }
    public void updatePriority(Event e, int chainIdx){
        for (int i = numPriorityChangePoints; i < chains.size(); i++){
            final Chain chain = chains.get(i);
            if (chain == null || chain.isEmpty() || i == chainIdx) {
                continue;
            }
            final Event event = chain.peekFirst();
            if (event.isEnabled() && isRacy(e, event)){
                numRacy++;
                statistics.reportNumRacy(numRacy);
                if (priorityChangePoints.containsKey(numRacy)){
                    Collections.swap(chains, i, priorityChangePoints.get(numRacy));
                }
            }
        }
    }
}
