/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 *
 * @author johsn
 */
public class TaskTimeOut extends Timeout
    {
        private final Address _scheduler;
        private final int _scheduler_task_id;
        protected TaskTimeOut(ScheduleTimeout st,int _scheduler_task_id,Address _scheduler)
        {
            super(st);
            this._scheduler_task_id=_scheduler_task_id;
            this._scheduler=_scheduler;
        }

    public Address getScheduler() {
        return _scheduler;
    }

    public int getScheduler_task_id() {
        return _scheduler_task_id;
    }
        
        
    }
