package org.mpisws.hitmc.server;

import org.mpisws.hitmc.api.SubnodeState;

public class Subnode {

    private final int id;
    private final int nodeId;
    private final boolean mainReceiver;
    private SubnodeState state = SubnodeState.PROCESSING;

    public  Subnode(final int id, final int nodeId, final boolean mainReceiver) {
        this.id = id;
        this.nodeId = nodeId;
        this.mainReceiver = mainReceiver;
    }

    public int getId() {
        return id;
    }

    public int getNodeId() {
        return nodeId;
    }

    public boolean isMainReceiver() {
        return mainReceiver;
    }

    public SubnodeState getState() {
        return state;
    }

    public void setState(final SubnodeState state) {
        this.state = state;
    }
}
