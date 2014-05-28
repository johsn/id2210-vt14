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
class FinishedTasks
{
    private final int _id;
    private final long _time_to_find_resources_for_this_task;
    private final int _cpu;
    private final int _mem;
    private final Address executor;
    private final Address scheduler;

    public FinishedTasks(int id,long time, int cpu, int mem, Address scheduler,Address executor)
    {
        this._id=id;
        this._time_to_find_resources_for_this_task = time;
        this._cpu = cpu;
        this._mem = mem;
        this.executor = executor;
        this.scheduler = scheduler;
    }

    public long getTime_to_find_resources_for_this_task() {
        return _time_to_find_resources_for_this_task;
    }
    
}
