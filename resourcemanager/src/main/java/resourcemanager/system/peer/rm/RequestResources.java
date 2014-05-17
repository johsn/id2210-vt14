package resourcemanager.system.peer.rm;

import java.util.List;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
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

    }
    
    public static class Response extends Message {

        private final boolean success;
        private final int _id;
        private final int _queue_size;
        public Response(Address source, Address destination, boolean success,int _id,int _queue_size) {
            super(source, destination);
            this.success = success;
            this._id=_id;
            this._queue_size=_queue_size;
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
}
