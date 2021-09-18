package org.mpisws.hitmc.zookeeper;

import org.mpisws.hitmc.api.Ensemble;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationPostLoadListener;
import org.mpisws.hitmc.util.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ZookeeperEnsemble implements Ensemble, SchedulerConfigurationPostLoadListener {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperEnsemble.class);

    private static final String ZOOKEEPER_QUORUM_PEER_MAIN = "org.apache.zookeeper.server.quorum.QuorumPeerMain";

    @Autowired
    private ZookeeperConfiguration zookeeperConfiguration;

    private int executionId;

    private final List<Process> processes = new ArrayList<>();

    @PostConstruct
    public void init() {
        zookeeperConfiguration.registerPostLoadListener(this);
    }

    @Override
    public void postLoadCallback() throws SchedulerConfigurationException {
        processes.addAll(Collections.<Process>nCopies(zookeeperConfiguration.getNumNodes(), null));
    }

    @Override
    public void startNode(final int nodeId) {
        if (null != processes.get(nodeId)) {
            LOG.warn("Node {} already started", nodeId);
            return;
        }

        LOG.debug("Starting node {}", nodeId);

        final File nodeDir = new File(zookeeperConfiguration.getWorkingDir(), executionId + File.separator + "nodes" + File.separator + nodeId);
        final File logDir = new File(nodeDir, "log");
        final File outputFile = new File(nodeDir, "out");
        final File confFile = new File(nodeDir, "conf");

        final String zookeeperLogDirOption = "-Dzookeeper.log.dir=" + logDir;
        final String appleAwtUIElementOption = "-Dapple.awt.UIElement=true"; // Suppress the Dock icon and menu bar on Mac OS X
        final String log4JConfigurationOption = "-Dlog4j.configuration=file:" + zookeeperConfiguration.getLog4JConfig();

        try {
            final Process process = ProcessUtil.startJavaProcess(zookeeperConfiguration.getWorkingDir(),
                    zookeeperConfiguration.getClasspath(), outputFile,
                    // Additional JVM options
                    zookeeperLogDirOption, appleAwtUIElementOption, log4JConfigurationOption,
                    // Class name and options
                    ZOOKEEPER_QUORUM_PEER_MAIN, confFile.getPath());
            processes.set(nodeId, process);
            LOG.debug("Started node {}", nodeId);
        } catch (final IOException e) {
            LOG.error("Could not start node " + nodeId, e);
        }
    }

    @Override
    public void stopNode(final int nodeId) {
        if (null == processes.get(nodeId)) {
            LOG.warn("Node {} is not running", nodeId);
            return;
        }

        LOG.debug("Stopping node {}", nodeId);
        final Process process = processes.get(nodeId);
        process.destroy();
        try {
            process.waitFor();
            LOG.debug("Stopped node {}", nodeId);
        } catch (final InterruptedException e) {
            LOG.warn("Main thread interrupted while waiting for a process to terminate", e);
        } finally {
            processes.set(nodeId, null);
        }
    }

    @Override
    public void startEnsemble() {
        LOG.debug("Starting the Zookeeper ensemble");
        for (int i = 0; i < zookeeperConfiguration.getNumNodes(); ++i) {
            startNode(i);
        }
        LOG.debug("Starting the Zookeeper clients");
        for (int i = 0; i < zookeeperConfiguration.getNumClients(); ++i) {
            startClient(i);
        }
    }

    @Override
    public void stopEnsemble() {
        LOG.debug("Stopping the Zookeeper ensemble");
        for (final Process process : processes) {
            if (null != process) {
                process.destroy();
            }
        }
        for (int i = 0; i < zookeeperConfiguration.getNumNodes(); ++i) {
            final Process process = processes.get(i);
            if (null != process) {
                try {
                    process.waitFor();
                    LOG.debug("Stopped node {}", i);
                } catch (final InterruptedException e) {
                    LOG.warn("Main thread interrupted while waiting for a process to terminate", e);
                } finally {
                    processes.set(i, null);
                }
            }
        }
    }

    @Override
    public void startClient(int client) {

    }

    @Override
    public void stopClient(int client) {

    }

    public void configureNode(final int nodeId) throws SchedulerConfigurationException {
        final File nodeDir = new File(zookeeperConfiguration.getWorkingDir(), executionId + File.separator + "nodes" + File.separator + nodeId);
        final File dataDir = new File(nodeDir, "data");

        // Create the data directory if it is missing
        dataDir.mkdirs();

        // Assemble the configuration file properties
        final Properties properties = new Properties();
        properties.setProperty("tickTime", String.valueOf(zookeeperConfiguration.getTickTime()));
        properties.setProperty("initLimit", String.valueOf(zookeeperConfiguration.getInitLimit()));
        properties.setProperty("syncLimit", String.valueOf(zookeeperConfiguration.getSyncLimit()));
        properties.setProperty("dataDir", dataDir.getPath());
        final int clientPort = zookeeperConfiguration.getClientPort() + nodeId;
        properties.setProperty("clientPort", String.valueOf(clientPort));
        for (int i = 0; i < zookeeperConfiguration.getNumNodes(); ++i) {
            final int quorumPort = zookeeperConfiguration.getBaseQuorumPort() + i;
            final int leaderElectionPort = zookeeperConfiguration.getBaseLeaderElectionPort() + i;
            properties.setProperty("server." + i, "localhost:" + quorumPort + ":" + leaderElectionPort);
        }

        final File confFile = new File(nodeDir, "conf");
        final File myidFile = new File(dataDir, "myid");
        try {
            final FileWriter confFileWriter = new FileWriter(confFile);
            properties.store(confFileWriter, "Automatically generated configuration for node " + nodeId);
            confFileWriter.close();

            final FileWriter myidFileWriter = new FileWriter(myidFile);
            myidFileWriter.write(String.valueOf(nodeId));
            myidFileWriter.close();
        }
        catch (final IOException e) {
            LOG.error("Could not write to a configuration file for node {}", nodeId);
            throw new SchedulerConfigurationException(e);
        }
    }

    @Override
    public void configureEnsemble(final int executionId) throws SchedulerConfigurationException {
        this.executionId = executionId;
        for (int i = 0; i < zookeeperConfiguration.getNumNodes(); ++i) {
            configureNode(i);
        }
    }

}
