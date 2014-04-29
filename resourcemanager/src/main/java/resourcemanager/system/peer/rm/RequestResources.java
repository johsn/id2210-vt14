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
    	private final int time;
        private final int numCpus;
        private final int amountMemInMb;
        private final int id;

        public Request(Address source, Address destination, int numCpus, int amountMemInMb, int time, int id) {
            super(source, destination);
            this.numCpus = numCpus;
            this.amountMemInMb = amountMemInMb;
            this.time = time;
            this.id = id;
        }
        
        public int getTime(){
        	return time;
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
    	private final int time;
    	private final int numCpus;
        private final int amountMemInMb;
        private final boolean success;
        private final int id;
        private int QueueSize;

        public int getQueueSize() {
            return QueueSize;
        }

        public void setQueueSize(int QueueSize) {
            this.QueueSize = QueueSize;
        }
        public Response(Address source, Address destination, boolean success, int numCpus, int amountMemInMB, int time, int id, int qsize) {
            super(source, destination);
            this.success = success;
            this.numCpus = numCpus;
            this.amountMemInMb = amountMemInMB;
            this.time = time;
            this.id = id;
            this.QueueSize=qsize;
        }
        
        public int getTime(){
        	return time;
        }
        
        public boolean isSuccess(){
        	return success;
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
    
    public static class Confirmation extends Message {
    	private final int time;
        private final int numCpus;
        private final int amountMemInMb;
        private final int id;

        public Confirmation(Address source, Address destination, int numCpus, int amountMemInMb, int time, int id) {
            super(source, destination);
            this.numCpus = numCpus;
            this.amountMemInMb = amountMemInMb;
            this.time = time;
            this.id = id;
        }
        
        public int getTime(){
        	return time;
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
    
    public static class RequestTimeout extends Timeout {
        private final Address destination;
        RequestTimeout(ScheduleTimeout st, Address destination) {
            super(st);
            this.destination = destination;
        }

        public Address getDestination() {
            return destination;
        }
    }
}