package org.mpisws.hitmc.server.statistics;

import org.mpisws.hitmc.server.statistics.Statistics;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class PCTCPStatistics implements Statistics {

    private int sumChains;
    private int maxChains;
    private int countChains;
    private String priorityChangePointsString;

    public void reportNumberOfChains(final int numChains) {
        sumChains += numChains;
        maxChains = Math.max(maxChains, numChains);
        countChains++;
    }

    private int sumEnabledEvents;
    private int maxEnabledEvents;
    private int countEnabledEvents;

    public void reportNumberOfEnabledEvents(final int numEnabledEvents) {
        sumEnabledEvents += numEnabledEvents;
        maxEnabledEvents = Math.max(maxEnabledEvents, numEnabledEvents);
        countEnabledEvents++;
    }

    private long startTime;

    @Override
    public void startTimer() {
        startTime = System.currentTimeMillis();
    }

    private long endTime;

    @Override
    public void endTimer() {
        endTime = System.currentTimeMillis();
    }

    private String result;

    @Override
    public void reportResult(final String result) {
        this.result = result;
    }

    private int totalExecutedEvents;

    @Override
    public void reportTotalExecutedEvents(final int totalExecutedEvents) {
        this.totalExecutedEvents = totalExecutedEvents;
    }

    private long seed;

    @Override
    public void reportRandomSeed(final long seed) {
        this.seed = seed;
    }

    private int maxEvents;

    public void reportMaxEvents(final int maxEvents) {
        this.maxEvents = maxEvents;
    }

    private int numPriorityChangePoints;

    public void reportNumPriorityChangePoints(final int numPriorityChangePoints) {
        this.numPriorityChangePoints = numPriorityChangePoints;
    }

    public void reportPriorityChangePoints(final Map<Integer, Integer> priorityChangePoints) {
        final SortedMap<Integer, Integer> reverse = new TreeMap<>();
        for (final Map.Entry<Integer, Integer> entry : priorityChangePoints.entrySet()) {
            reverse.put(entry.getValue(), entry.getKey());
        }
        final StringBuilder sb = new StringBuilder("[");
        String sep = "";
        for (final Map.Entry<Integer, Integer> entry : reverse.entrySet()) {
            sb.append(sep);
            sb.append(entry.getValue());
            sep = ", ";
        }
        sb.append("]");
        priorityChangePointsString = sb.toString();
    }

    @Override
    public String toString() {
        final double avgChains = ((double) sumChains) / countChains;
        final double avgEnabledEvents = ((double) sumEnabledEvents) / countEnabledEvents;
        final long totalTime = endTime - startTime;
        return "PCTCPStatistics{" +
                "\n  randomSeed = " + seed +
                "\n, maxEvents = " + maxEvents +
                "\n, numPriorityChangePoints = " + numPriorityChangePoints +
                "\n, priorityChangePoints = " + priorityChangePointsString +
                "\n, totalEvents = " + totalExecutedEvents +
                "\n, averageChains = " + avgChains +
                "\n, maxChains = " + maxChains +
                "\n, averageEnabledEvents = " + avgEnabledEvents +
                "\n, maxEnabledEvents = " + maxEnabledEvents +
                "\n, totalTime = " + totalTime + " ms" +
                "\n, result = " + result +
                "\n}";
    }
}
