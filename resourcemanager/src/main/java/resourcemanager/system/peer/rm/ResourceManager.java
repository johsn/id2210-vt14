package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.BatchJob;
import common.simulation.RequestResource;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.ws.handler.HandlerResolver;

import org.apache.commons.math.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import system.peer.RmPort;
import tman.system.peer.tman.TManSample;
import tman.system.peer.tman.TManSamplePort;

/**
 * Should have some comments here.
 *
 * @author jdowling
 */
public final class ResourceManager extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);
    Positive<RmPort> indexPort = positive(RmPort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanPort = positive(TManSamplePort.class);
    ArrayList<Address> neighbors = new ArrayList<Address>();
    private Address self;
    private RmConfiguration configuration;
    Random random;
    private AvailableResources availableResources;
    private static final int PROBES = 10; 
    
    private ArrayList<TaskInformation> UnScheduledTasks = new ArrayList<TaskInformation>();
    private ArrayList<TaskInformation> RunningTasks = new ArrayList<TaskInformation>();
    private ArrayList<TaskInformation> queue = new ArrayList<TaskInformation>();
    
    Comparator<PeerDescriptor> peerAgeComparator = new Comparator<PeerDescriptor>() {
        @Override
        public int compare(PeerDescriptor t, PeerDescriptor t1) {
            if (t.getAge() > t1.getAge()) {
                return 1;
            } else {
                return -1;
            }
        }
    };

	
    public ResourceManager() {

        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleRequestResource, indexPort);
        subscribe(handleBatchJob, indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleTManSample, tmanPort);
        subscribe(handleResourceAllocationConfirmation, networkPort);
        subscribe(handleTimeout, timerPort);
        
    }
	
    Handler<RmInit> handleInit = new Handler<RmInit>() {
        @Override
        public void handle(RmInit init) {
            self = init.getSelf();
            configuration = init.getConfiguration();
            random = new Random(init.getConfiguration().getSeed());
            availableResources = init.getAvailableResources();
            long period = configuration.getPeriod();
            availableResources = init.getAvailableResources();
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateTimeout(rst));
            trigger(rst, timerPort);


        }
    };


    Handler<UpdateTimeout> handleUpdateTimeout = new Handler<UpdateTimeout>() {
        @Override
        public void handle(UpdateTimeout event) {

            // pick a random neighbour to ask for index updates from. 
            // You can change this policy if you want to.
            // Maybe a gradient neighbour who is closer to the leader?
            if (neighbors.isEmpty()) {
                return;
            }
            Address dest = neighbors.get(random.nextInt(neighbors.size()));


        }
    };

    Handler<RequestResources.Confirmation> handleResourceAllocationConfirmation = new Handler<RequestResources.Confirmation>() {
        @Override
        public void handle(RequestResources.Confirmation event) {
            
            
                if(availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb()))
                {
                    availableResources.allocate(event.getNumCpus(), event.getAmountMemInMb());
                    RunningTasks.add(new TaskInformation(event.getNumCpus(), event.getAmountMemInMb(), event.getId(),event.getTime()));
                    startTimer(event.getTime(), event.getId());
                }
                else
                {
                    synchronized(queue)
                    {
                        queue.add(new TaskInformation(event.getNumCpus(),event.getAmountMemInMb(),event.getId(),event.getTime()));
                    }
                }
        }
    };

    Handler<RequestResources.Request> handleResourceAllocationRequest = new Handler<RequestResources.Request>() {
        @Override
        public void handle(RequestResources.Request event) {
            trigger(new RequestResources.Response(self, event.getSource(), 
            		availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb()), 
            		event.getNumCpus(), event.getAmountMemInMb(), event.getTime(), event.getId(),queue.size()), networkPort);
        }
    };
    
    
    
    Handler<RequestResources.Response> handleResourceAllocationResponse = new Handler<RequestResources.Response>() {
        @Override
        public void handle(RequestResources.Response event)
        {
            synchronized(UnScheduledTasks)
            {
                for(TaskInformation task : UnScheduledTasks)
                {
                    if(task.getId()==event.getId())
                    {
                        if(event.isSuccess())
                        {
                            UnScheduledTasks.remove(task);
                            trigger(new RequestResources.Confirmation(self, event.getSource(), event.getNumCpus(), event.getAmountMemInMb(), event.getTime(), event.getId()),networkPort);
                        }
                        else
                        {
                            task.getQueue_sizes().add(new NodeAddressAndQueueInfo(event.getQueueSize(),event.getSource()));
                            if(task.getQueue_sizes().size()==PROBES)
                            {
                                int shortestQueue = Integer.MAX_VALUE;
                                for(NodeAddressAndQueueInfo info : task.getQueue_sizes())
                                {
                                    if(info.getQueue()<shortestQueue)
                                    {
                                        shortestQueue=info.getQueue();
                                    }
                                }
                                for(NodeAddressAndQueueInfo node : task.getQueue_sizes())
                                {
                                    if(node.getQueue()==shortestQueue)
                                    {
                                        UnScheduledTasks.remove(task);
                                        trigger(new RequestResources.Confirmation(self, node.getSource(), event.getNumCpus(), event.getAmountMemInMb(), event.getTime(), event.getId()),networkPort);
                                    }
                                }
                            }
                            else
                            {
                                break;
                            }
                        }
                        break;
                    }
                }
            }
        }
    };
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            System.out.println("Received samples: " + event.getSample().size());
            System.out.println("Node [" + self.getIp().getHostAddress() + "] " + "has Queue size -> ["+queue.size()+"]");
            // receive a new list of neighbours
            neighbors.clear();
            neighbors.addAll(event.getSample());

        }
    };
	
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            
            System.out.println("Allocate resources: " + event.getNumCpus() + " + " + event.getMemoryInMbs());
            // TODO: Ask for resources from neighbours
            // by sending a ResourceRequest
            int id = 0;
            synchronized(UnScheduledTasks)
            {
                for(int i = 0; i < UnScheduledTasks.size(); i++)
                {
                    id = Math.max(id, UnScheduledTasks.get(i).getId() + 1);
                }
                UnScheduledTasks.add(new TaskInformation(event.getNumCpus(),event.getMemoryInMbs(),id,event.getTimeToHoldResource()));
            }    
            ArrayList<Address> rN = new ArrayList<Address>(neighbors);
            for(int i = 0; i < neighbors.size() - PROBES; i++){
            	rN.remove(random.nextInt(rN.size()));
            }
            for(Address dest : rN)
            {
                    synchronized(UnScheduledTasks)
                    {
                        for(TaskInformation task : UnScheduledTasks)
                        {
                            RequestResources.Request req = new RequestResources.Request(self, dest,
                            task.getNumCpus(), task.getMemoryInMbs(), task.getTime(), task.getId());
                            trigger(req, networkPort);
                        }
                    }
            }
        }
    };
    
     Handler<BatchJob> handleBatchJob = new Handler<BatchJob>() {
        @Override
        public void handle(BatchJob event)
        {
            int requested_machines = event.getMachines();
            int requested_cpus = event.getNumCpus();
            int requested_mem = event.getMemoryInMbs();
            
            System.out.println("Start BatchJob for machines: " + requested_machines + "cpus: "+ requested_cpus + "memory: "+ requested_mem);
            
            for(int i=0; i< requested_machines;i++)
            {
                RequestResource r = new RequestResource(self.getId(),event.getNumCpus(),event.getMemoryInMbs(),event.getTimeToHoldResource());
                handleRequestResource.handle(r);
            }
            
            
        }
    };
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            // TODO: 
        }
    };
    
    private Handler<CheckTimeout> handleTimeout = new Handler<CheckTimeout>() {
		@Override
		public void handle(CheckTimeout event) {
			for(TaskInformation a : RunningTasks)
                        {
				if(a.getId() == event.getId())
                                {
					availableResources.release(a.getNumCpus(), a.getMemoryInMbs());
					System.out.println("["+self.getIp().getHostAddress()+"]" + " Released"+ "["+a.getNumCpus()+"]" + "["+a.getMemoryInMbs()+"]");
					RunningTasks.remove(a);
				}
                                else
                                {
                                    System.out.println("-------------------------------- hittade ej");
                                }
                                synchronized(queue)
                                {
                                    if(queue.size()>0)
                                    {
                                        if(availableResources.isAvailable(queue.get(0).getNumCpus(), queue.get(0).getMemoryInMbs()))
                                        {
                                            availableResources.allocate(queue.get(0).getNumCpus(), queue.get(0).getMemoryInMbs());
                                            RunningTasks.add(queue.get(0));
                                            startTimer(queue.get(0).getTime(),queue.get(0).getId());
                                            queue.remove(queue.get(0));
                                            break;
                                        }
                                    }
                                    break;
                                }
			}
			
		}
	};
    
	private void startTimer(long delay, int id) {
		ScheduleTimeout st = new ScheduleTimeout(delay);
		st.setTimeoutEvent(new CheckTimeout(st, id));
		trigger(st, timerPort);
	}

}