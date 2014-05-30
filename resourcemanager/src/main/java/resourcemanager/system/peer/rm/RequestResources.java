package resourcemanager.system.peer.rm;

import java.util.ArrayList;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 * User: jdowling
 */
public class RequestResources  {

    public static class Request extends Message {

        private final int numCpus;
        private final int amountMemInMb;
        private final int id;
        private boolean _batch_task;
        private int _batch_id;

        public int getBatch_id() {
            return _batch_id;
        }

        public void setBatch_id(int _batch_id) {
            this._batch_id = _batch_id;
        }

        public Request(Address source, Address destination, int numCpus, int amountMemInMb,int id) {
            super(source, destination);
            this.numCpus = numCpus;
            this.amountMemInMb = amountMemInMb;
            this.id=id;
        }

        public int getAmountMemInMb() {
            return amountMemInMb;
        }

        public int getNumCpus() {
            return numCpus;
        }

        public int getId() {
            return id;
        }

        public boolean isBatch_task() {
            return _batch_task;
        }

        public void setBatch_task(boolean _batch_task) {
            this._batch_task = _batch_task;
        }

    }
    
    public static class Response extends Message {

        private final boolean success;
        private final int _id;
        private final int _queue_size;
        private boolean _batch_task;
        private int _batch_id;
        private long _resources_found_time;
        public Response(Address source, Address destination, boolean success,int _id,int _queue_size) {
            super(source, destination);
            this.success = success;
            this._id=_id;
            this._queue_size=_queue_size;
        }

        public long getResources_found_time() {
            return _resources_found_time;
        }

        public void setResources_found_time(long _resources_found_time) {
            this._resources_found_time = _resources_found_time;
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

        public void setBatch_id(int _batch_id) {
            this._batch_id = _batch_id;
        }

        public boolean isSuccess() {
            return success;
        }

        public int getId() {
            return _id;
        }

        public int getQueue_size() {
            return _queue_size;
        }
    }
    
    public static class Confirm extends Message {
        private final int _id;
        private final int _cpus;
        private final int _mem;
        private final int _task_time;
        private boolean batch_task;
        private int batch_id;
        public Confirm(Address source, Address destination,int _cpus,int _mem,int _task_time, int _id) {
            super(source, destination);
            this._id=_id;
            this._cpus=_cpus;
            this._mem=_mem;
            this._task_time=_task_time;
        }

        public boolean isBatch_task() {
            return batch_task;
        }

        public void setBatch_task(boolean batch_task) {
            this.batch_task = batch_task;
        }

        public int getBatch_id() {
            return batch_id;
        }

        public void setBatch_id(int batch_id) {
            this.batch_id = batch_id;
        }
        
        public int getTask_time() {
            return _task_time;
        }

        public int getId() {
            return _id;
        }

        public int getCpus() {
            return _cpus;
        }

        public int getMem() {
            return _mem;
        }
    }

    public static class Ping extends Message{
        
        private final int _id;
        
        public Ping(Address source,Address destination,int _id)
        {
            super(source,destination);
            this._id=_id;
        }

        public int getId() {
            return _id;
        }
        
    }
    
     public static class Pong extends Message{
        
        private final int _id;
        private final boolean _running;
        private boolean _queued;
        
        public Pong(Address source,Address destination,boolean _running,int _id)
        {
            super(source,destination);
            this._id=_id;
            this._running=_running;
        }

        public boolean isQueued() {
            return _queued;
        }

        public void setQueued(boolean _queued) {
            this._queued = _queued;
        }

        public boolean isRunning() {
            return _running;
        }

        public int getId() {
            return _id;
        }
        
    }
     
     public static class FindBestPeerToRunTask extends Message{
         
        private final int numCpus;
        private final int amountMemInMb;
        private final int id;
        private Address _Scheduler;
        private boolean _batch_task;
        private int _batch_id;
        private final String _type;
        private ArrayList<Address> _dont_schedule_here = new ArrayList<Address>();
        
        
        public FindBestPeerToRunTask(Address source,Address destination, String type,int numCpus,int Mem,int id)
        {
            super(source,destination);
            this.numCpus = numCpus;
            this.amountMemInMb = Mem;
            this.id = id;
            this._type = type;
        }

        public ArrayList<Address> getDont_schedule_here() {
            return _dont_schedule_here;
        }

        public Address getScheduler() {
            return _Scheduler;
        }

        public void setScheduler(Address _Scheduler) {
            this._Scheduler = _Scheduler;
        }

        public String getType() {
            return _type;
        }

        public int getNumCpus() {
            return numCpus;
        }

        public int getAmountMemInMb() {
            return amountMemInMb;
        }

        public int getId() {
            return id;
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

        public void setBatch_id(int _batch_id) {
            this._batch_id = _batch_id;
        }
        
    }

    public static class SetExpectedResponses extends Message{

        private final int _id;
        private final int _expected_responses;
        public SetExpectedResponses(Address src, Address dest,int id,int eresponses)
        {
            super(src,dest);
            this._expected_responses = eresponses;
            this._id = id;
            
        }

        public int getId() {
            return _id;
        }

        public int getExpected_responses() {
            return _expected_responses;
        }
        
    }

    public static class ResourceFound extends Message {

        private final long _time_found_resource;
        private final int _id;
        public ResourceFound(Address src, Address dest,int id, long time) {
            super(src,dest);
            this._id = id;
            this._time_found_resource = time;
        }

        public long getTime_found_resource() {
            return _time_found_resource;
        }

        public int getId() {
            return _id;
        }
        
    }
    
    public static class ThisBatchTaskIsNotrunningHereAnymore extends Message {
        
        private final int batch_id;
        
        public ThisBatchTaskIsNotrunningHereAnymore(Address src, Address dest,int batch_id) {
            super(src,dest);
            this.batch_id = batch_id;
        }

        public int getBatchId() {
            return batch_id;
        }
        
    }
     
    
}
