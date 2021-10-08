package org.mpisws.hitmc.api.configuration;

public interface SchedulerConfiguration {

    void load(String[] args) throws SchedulerConfigurationException;

    void registerPostLoadListener(SchedulerConfigurationPostLoadListener listener);

    void notifyPostLoadListeners() throws SchedulerConfigurationException;

    int getNumNodes();

    int getNumCrashes();

    int getNumReboots();

    int getNumClients();

    int getNumReaders();

    int getNumWriters();

    int getMaxEvents();

    int getNumPriorityChangePoints();

    boolean hasRandomSeed();

    long getRandomSeed();

    long getTickTime();

    int getNumExecutions();

    String getExecutionFile();

    String getStatisticsFile();

    String getSchedulingStrategy();

    void configureNode(int executionId, int nodeId, String tag) throws SchedulerConfigurationException;



}
