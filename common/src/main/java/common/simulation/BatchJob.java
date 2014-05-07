package common.simulation;

import se.sics.kompics.Event;

public final class BatchJob extends Event {
    
    private final long id;
    private final int numCpus;
    private final int memoryInMbs;
    private final int timeToHoldResource;
    private final int machines;

    public BatchJob(long id,int machines, int numCpus, int memoryInMbs, int timeToHoldResource) {
        this.machines=machines;
        this.id = id;
        this.numCpus = numCpus;
        this.memoryInMbs = memoryInMbs;
        this.timeToHoldResource = timeToHoldResource;
    }

    public int getMachines() {
        return machines;
    }

    public long getId() {
        return id;
    }

    public int getTimeToHoldResource() {
        return timeToHoldResource;
    }

    public int getMemoryInMbs() {
        return memoryInMbs;
    }

    public int getNumCpus() {
        return numCpus;
    }

}