package common.simulation;

import java.util.ArrayList;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;

public final class RequestResource extends Event {
    
    private final long id;
    private final int numCpus;
    private final int memoryInMbs;
    private final int timeToHoldResource;
    private boolean _batch_task =false;
    private int _batch_id;
    private boolean empty = false;

    public RequestResource(long id, int numCpus, int memoryInMbs, int timeToHoldResource) {
        this.id = id;
        this.numCpus = numCpus;
        this.memoryInMbs = memoryInMbs;
        this.timeToHoldResource = timeToHoldResource;
    }
    
    public RequestResource(boolean empty) {
        this.id = 0;
        this.numCpus = 0;
        this.memoryInMbs = 0;
        this.timeToHoldResource = 0;
        this.empty = empty;
    }

    public boolean isEmpty() {
        return empty;
    }

    public boolean isBatch_task() {
        return _batch_task;
    }

    public void setBatch_task(boolean _batch_task) {
        this._batch_task = _batch_task;
    }

    public int getBatch_id() {
        return _batch_id;
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

    public void setBatchId(int _batch_id) {
        this._batch_id = _batch_id;
    }

}
