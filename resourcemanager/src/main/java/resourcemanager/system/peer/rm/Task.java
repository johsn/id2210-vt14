/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;

/**
 *
 * @author johsn
 */
class Task
{
    private final int _id;
    private final int _cpus;
    private final int _memory;
    private final int _task_time;
    private int _expected_responses;
    private final Address _scheduler;
    private Address _potential_executor;
    private Address _shorest_queue_peer;
    private int responses = 0;
    private int shortest_queue = Integer.MAX_VALUE;
    private boolean _ponged;
    private boolean _running;
    private  boolean _batch_task;
    private int batch_id;

    
    private long _time_to_find_resource;
    private long _start_time;
    private long _endtime;

    public long getTime_to_find_resource() {
        return _time_to_find_resource;
    }

    public void setTime_to_find_resource(long _time_to_find_resource) {
        this._time_to_find_resource = _time_to_find_resource;
    }

    public long getStart_time() {
        return _start_time;
    }

    public void setStart_time(long _start_time) {
        this._start_time = _start_time;
    }

    public long getEndtime() {
        return _endtime;
    }

    public void setEndtime(long _endtime) {
        this._endtime = _endtime;
    }
    
    public int getBatch_id() {
        return batch_id;
    }

    public void setBatch_id(int batch_id) {
        this.batch_id = batch_id;
    }

    public boolean isBatch_task() {
        return _batch_task;
    }

    public void setBatch_task(boolean _batch_task) {
        this._batch_task = _batch_task;
    }

    public Address getShorest_queue_peer() {
        return _shorest_queue_peer;
    }

    public void setShorest_queue_peer(Address _shorest_queue_peer) {
        this._shorest_queue_peer = _shorest_queue_peer;
    }

    public int getExpected_responses() {
        return _expected_responses;
    }

    public void setExpected_responses(int _expected_responses) {
        this._expected_responses = _expected_responses;
    }

    public boolean isRunning() {
        return _running;
    }

    public void setRunning(boolean _running) {
        this._running = _running;
    }

    public boolean isPonged() {
        return _ponged;
    }

    public void setPonged(boolean _ponged) {
        this._ponged = _ponged;
    }
    
    
    public Address getPotentialExecutor() {
        return _potential_executor;
    }

    public void setPotentialExecutor(Address runner) {
        this._potential_executor = runner;
    }

    public int getId() {
        return _id;
    }

    public int getCpus() {
        return _cpus;
    }

    public int getMemory() {
        return _memory;
    }

    public int getResponses() {
        return responses;
    }

    public void setResponses(int responses) {
        this.responses = responses;
    }

    public int getShortest_queue() {
        return shortest_queue;
    }

    public void setShortest_queue(int shortest_queue) {
        this.shortest_queue = shortest_queue;
    }

    public int getTask_time() {
        return _task_time;
    }

    public Address getScheduler() {
        return _scheduler;
    }

    public Task(int _id, int _cpus, int _memory,int _task_time,Address _requestor) {
        this._id = _id;
        this._cpus = _cpus;
        this._memory = _memory;
        this._task_time=_task_time;
        this._scheduler = _requestor;
    }
}
