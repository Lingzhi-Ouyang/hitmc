package org.mpisws.hitmc.server;

import org.mpisws.hitmc.api.Event;

public interface SchedulingStrategy {

    void add(Event event);

    boolean hasNextEvent();

    Event nextEvent();

}
