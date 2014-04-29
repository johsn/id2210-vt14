package resourcemanager.system.peer.rm;

import java.util.ArrayList;

public class TaskInformation {
	private int numCpus;
        private int memoryInMbs;
        private int id;
        private int time;
        private ArrayList<NodeAddressAndQueueInfo> queue_sizes = new ArrayList<NodeAddressAndQueueInfo>();
        
    public TaskInformation(int numCpus, int memoryInMbs, int id, int time) {
        this.numCpus = numCpus;
        this.memoryInMbs = memoryInMbs;
        this.id = id;
        this.time = time;
    }

    public ArrayList<NodeAddressAndQueueInfo> getQueue_sizes() {
        return queue_sizes;
    }

    public int getNumCpus() {
        return numCpus;
    }

    public void setNumCpus(int numCpus) {
        this.numCpus = numCpus;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }
    
    public int getMemoryInMbs(){
    	return memoryInMbs;
    }
    
    public void setMemoryInMbs(int mem)
    {
        memoryInMbs=mem;
    }
}