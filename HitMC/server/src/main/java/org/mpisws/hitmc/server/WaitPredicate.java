package org.mpisws.hitmc.server;

public interface WaitPredicate {

    boolean isTrue();

    String describe();
}
