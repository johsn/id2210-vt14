package resourcemanager.system.peer.rm;

public class IDCPUMem {
	private int numCpus;
    private int memoryInMbs;
    private int id;
    
    public IDCPUMem(int numCpus, int memoryInMbs, int id) {
        this.numCpus = numCpus;
        this.memoryInMbs = memoryInMbs;
        this.id = id;
    }
    
    public int getNumCPUs(){
    	return numCpus;
    }
    
    public int getMemoryInMbs(){
    	return memoryInMbs;
    }
    
    public int getID(){
    	return id;
    }
}