/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 *
 * @author johsn
 */
class PongTimeOut extends Timeout{

    private final int _task_id;
    
    public PongTimeOut(ScheduleTimeout st, int id) 
    {
        super(st);
        this._task_id=id;
    }

    public int getTask_id() {
        return _task_id;
    }
    
}
