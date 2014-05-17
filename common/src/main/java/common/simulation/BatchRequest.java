package common.simulation;

import se.sics.kompics.Event;

public final class BatchRequest extends Event {
    
    private final long _id;
    private final int _numCpus;
    private final int _memoryInMbs;
    private final int _timeToHoldResource;
    private final int _machines;

    public BatchRequest(long id, int machines, int numCpus, int memoryInMbs, int timeToHoldResource) {
        this._id = id;
        this._numCpus = numCpus;
        this._memoryInMbs = memoryInMbs;
        this._timeToHoldResource = timeToHoldResource;
        this._machines=machines;
    }

    public int getMachines() {
        return _machines;
    }

    public long getId() {
        return _id;
    }

    public int getTimeToHoldResource() {
        return _timeToHoldResource;
    }

    public int getMemoryInMbs() {
        return _memoryInMbs;
    }

    public int getNumCpus() {
        return _numCpus;
    }

}