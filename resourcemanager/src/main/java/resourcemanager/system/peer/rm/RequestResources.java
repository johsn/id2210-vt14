package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

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
        public Response(Address source, Address destination, boolean success,int _id,int _queue_size) {
            super(source, destination);
            this.success = success;
            this._id=_id;
            this._queue_size=_queue_size;
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
        public Confirm(Address source, Address destination,int _cpus,int _mem,int _task_time, int _id) {
            super(source, destination);
            this._id=_id;
            this._cpus=_cpus;
            this._mem=_mem;
            this._task_time=_task_time;
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
        
        public Pong(Address source,Address destination,boolean _running,int _id)
        {
            super(source,destination);
            this._id=_id;
            this._running=_running;
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
        
        
        public FindBestPeerToRunTask(Address source,Address destination, String type,int numCpus,int Mem,int id)
        {
            super(source,destination);
            this.numCpus = numCpus;
            this.amountMemInMb = Mem;
            this.id = id;
            this._type = type;
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
     
    
}
