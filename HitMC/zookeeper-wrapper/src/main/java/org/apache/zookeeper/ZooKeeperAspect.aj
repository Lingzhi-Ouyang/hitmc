package org.apache.zookeeper;

import org.mpisws.hitmc.api.SchedulerRemote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public aspect ZooKeeperAspect {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperAspect.class);

    private final SchedulerRemote scheduler;

    public ZooKeeperAspect() {
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

    public SchedulerRemote getScheduler() {
        return scheduler;
    }

    // Intercept ZooKeeper.getChildren()

    pointcut getChildren(String p, boolean w): execution(* ZooKeeper.getChildren(String, boolean))
            && args(p, w);

    before(String p, boolean w): getChildren(p, w) {
        LOG.debug("Getting children of " + p);
        ZooKeeperDemo zooKeeperDemo = null;
        try {
            zooKeeperDemo = new ZooKeeperDemo();
            zooKeeperDemo.testGet();
        } catch (IOException | KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

//    after(String p, boolean w) returning: getChildren(p, w) {
//        LOG.debug("Getting children of " + p);
//    }


}
