package org.mpisws.hitmc.api;

public class TestingDef {
    public interface RetCode {
        int CLIENT_INITIALIZATION_NOT_DONE = 0;
        int NODE_CRASH = -1;
        int NODE_PAIR_IN_PARTITION = -2;
    }
}
