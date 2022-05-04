package org.mpisws.hitmc.server.scheduler;

import org.mpisws.hitmc.server.event.Event;
import org.mpisws.hitmc.server.event.MessageEvent;
import org.mpisws.hitmc.server.event.NodeCrashEvent;
import org.mpisws.hitmc.server.statistics.POSstatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class POSstrategy implements SchedulingStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(POSstrategy.class);
    private POSstatistics statistics;
    private Random random;
    private Map<Event, Double> priorities;
    private int maxEvents;

    public POSstrategy(int maxEvents, Random random, POSstatistics statistics) {
        this.random = random;
        this.maxEvents = maxEvents;
        this.statistics = statistics;
        priorities = new HashMap<>();
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

        nextEvent = getHighPriority();
        nextEventPrepared = true;
        if (nextEvent != null){
            priorities.remove(nextEvent);
            updatePriorities(nextEvent);
        }
    }

    @Override
    public void add(final Event event){
        if (!priorities.keySet().contains(event)){
            double pr = random.nextDouble();
            priorities.put(event, pr);
            LOG.debug("--- Added transition: " + event.toString() + " with ID: ‌" + event.getId() + " with priority: " + pr);
        }

        if (nextEventPrepared && nextEvent == null){
            nextEventPrepared = false;
        }
    }

    private void updatePriorities(Event recentEvent)
    {
        List<Event> keys = new ArrayList<>(priorities.keySet());
            for (Event e : keys) {
                if (e.isEnabled()) {
                    if (isRacy(e, recentEvent)) {
                        double pr = random.nextDouble();
                        priorities.put(e, pr);
                        LOG.debug("--- Updated priority of: " + e.getId() + " with id: " + ((Event) recentEvent).getId() + " with priority: " + pr);
                    }
                }
            }
    }


    private Event getHighPriority() {
        double mx = -1;
        Event res = null;
        for (Event e : priorities.keySet()) {
            //LOG.debug(e.getId() + " is enabled");
            if (mx < priorities.get(e) && e.isEnabled()) {
                mx = priorities.get(e);
                res = e;
            }
        }
        return res;
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
}