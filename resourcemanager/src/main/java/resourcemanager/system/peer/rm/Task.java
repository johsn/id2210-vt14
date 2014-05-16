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
