package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
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
    ArrayList<Address> neighbours = new ArrayList<Address>();
    private Address self;
    private RmConfiguration configuration;
    Random random;
    private AvailableResources availableResources;
    private static final int N_PROBES = 2; 
    private ArrayList<IDCPUMem> currentlyRunning = new ArrayList<IDCPUMem>();
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
            if (neighbours.isEmpty()) {
                return;
            }
            Address dest = neighbours.get(random.nextInt(neighbours.size()));


        }
    };

    Handler<RequestResources.Confirmation> handleResourceAllocationConfirmation = new Handler<RequestResources.Confirmation>() {
        @Override
        public void handle(RequestResources.Confirmation event) {
        	availableResources.allocate(event.getNumCpus(), event.getAmountMemInMb());
        	currentlyRunning.add(new IDCPUMem(event.getNumCpus(), event.getAmountMemInMb(), event.getId()));
        	startTimer(event.getTime(), event.getId());
        }
    };

    Handler<RequestResources.Request> handleResourceAllocationRequest = new Handler<RequestResources.Request>() {
        @Override
        public void handle(RequestResources.Request event) {
            trigger(new RequestResources.Response(self, event.getSource(), 
            		availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb()), 
            		event.getNumCpus(), event.getAmountMemInMb(), event.getTime(), event.getId()), networkPort);
        }
    };
    
    private ArrayList<Integer> waitingForIds = new ArrayList<Integer>();
    
    Handler<RequestResources.Response> handleResourceAllocationResponse = new Handler<RequestResources.Response>() {
        @Override
        public void handle(RequestResources.Response event) {
            if(waitingForIds.contains(event.getId()) && event.isSuccess()){ //TODO ändra om vi lägger till kö
            	waitingForIds.remove((Integer)event.getId());
            	trigger(new RequestResources.Confirmation(self, event.getSource(), 
                		event.getNumCpus(), event.getAmountMemInMb(), event.getTime(), event.getId()), networkPort);
            }
        }
    };
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            System.out.println("Received samples: " + event.getSample().size());
            
            // receive a new list of neighbours
            neighbours.clear();
            neighbours.addAll(event.getSample());

        }
    };
	
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            
            System.out.println("Allocate resources: " + event.getNumCpus() + " + " + event.getMemoryInMbs());
            // TODO: Ask for resources from neighbours
            // by sending a ResourceRequest
            int id = 0;
            for(int i = 0; i < waitingForIds.size(); i++){
            	id = Math.max(id, waitingForIds.get(i) + 1);
            }
            waitingForIds.add(id);
            ArrayList<Address> rN = new ArrayList<Address>(neighbours);
            for(int i = 0; i < neighbours.size() - N_PROBES; i++){
            	rN.remove(random.nextInt(rN.size()));
            }
            for(Address dest : rN){
	            RequestResources.Request req = new RequestResources.Request(self, dest,
	            event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(), id);
	            trigger(req, networkPort);
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
			for(IDCPUMem a : currentlyRunning){
				if(a.getID() == event.getId()){
					availableResources.release(a.getNumCPUs(), a.getMemoryInMbs());
					System.out.println("-------------------------RELEASEEEEEEE!!!");
					currentlyRunning.remove(a);
					return;
				}
			}
			System.out.println("-------------------------------- hittade ej");
		}
	};
    
	private void startTimer(long delay, int id) {
		System.out.println("-------------------------------- start timer: " + delay);
		ScheduleTimeout st = new ScheduleTimeout(delay);
		st.setTimeoutEvent(new CheckTimeout(st, id));
		trigger(st, timerPort);
	}

}