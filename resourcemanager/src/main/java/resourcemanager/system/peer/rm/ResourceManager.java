package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.BatchRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
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
    ArrayList<PeerDescriptor> _tmanCpu = new ArrayList<PeerDescriptor>();
    ArrayList<PeerDescriptor> _tmanMem = new ArrayList<PeerDescriptor>();
    private Address self;
    private RmConfiguration configuration;
    Random random;
    private AvailableResources availableResources;
    // When you partition the index you need to find new nodes
    // This is a routing table maintaining a list of pairs in each partition.
    private Map<Integer, List<PeerDescriptor>> routingTable;

    private static final int PROBES = 4;
    private static final int MAX_RESPONSE_TIMEOUT = 2000;
    private int _current_task_id = 0;
    private int _current_batch_id = 0;
    private final Object _lock_task_id = new Object();
    private final Object _lock_responses = new Object();
    private final Object _lock_bacth_id = new Object();
    private final boolean _gradient = true;

    private List<Task> _idle_tasks = Collections.synchronizedList(new ArrayList());
    private List<Task> _non_idle_tasks = Collections.synchronizedList(new ArrayList());

    private List<Task> _tasks_runnning_on_this_machine = Collections.synchronizedList(new ArrayList());
    private List<Task> _queue_for_this_machine = Collections.synchronizedList(new ArrayList());
    private ArrayList<FinishedTasks> _finished_tasks = new ArrayList<FinishedTasks>();

    private int getCurrentTaskId() {
        synchronized (_lock_task_id) {
            int tmp = this._current_task_id;
            this._current_task_id = this._current_task_id + 1;
            return tmp;
        }
    }
    
    private int getCurrentBatchId() {
            synchronized(_lock_bacth_id)
                    {
                        int tmp = _current_batch_id;
                        _current_batch_id = _current_batch_id + 1;
                        return tmp;
                    }
    }
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
        subscribe(handleBatchRequest,indexPort);
        subscribe(handleAvarageTime,indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleTaskTimeOut, timerPort);
        subscribe(handlePongTimeOut, timerPort);
        subscribe(handleRequestTimeOut,timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handlePing, networkPort);
        subscribe(handlePong, networkPort);
        subscribe(handleConfirm, networkPort);
        subscribe(handleExpectedResponses,networkPort);
        subscribe(handleBestPeerRequest,networkPort);
        subscribe(handleResourceFound,networkPort);
        subscribe(handleTManSampleCpu, tmanPort);
        subscribe(handleTManSampleMem, tmanPort);
    }

    Handler<RmInit> handleInit = new Handler<RmInit>() {
        @Override
        public void handle(RmInit init) {
            self = init.getSelf();
            configuration = init.getConfiguration();
            routingTable = new HashMap<Integer, List<PeerDescriptor>>(configuration.getNumPartitions());
            random = new Random(init.getConfiguration().getSeed());
            availableResources = init.getAvailableResources();
            long period = configuration.getPeriod();
            availableResources = init.getAvailableResources();
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateTimeout(rst));
            trigger(rst, timerPort);

        }
    };
    
    Handler<TerminateExperiment> handleAvarageTime = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment event) {
            
            ArrayList<Long> results = new ArrayList<Long>();
            for(FinishedTasks f : _finished_tasks)
            {
                results.add(f.getTime_to_find_resources_for_this_task());
            }
            long sum = 0;
            for(Long l : results)
            {
                sum += l;
            }
            long avarage = sum / results.size();
            System.out.println(" ");
            System.out.println("################################ AVARAGE FOR RM "+ self.getIp().getHostAddress()+" ##############################");
            System.out.println("Avarage = " + avarage);
            System.out.println("#################################################################################################################");
            System.out.println(" ");

        }
    };
    
    Handler<RequestResources.ResourceFound> handleResourceFound = new Handler<RequestResources.ResourceFound>() {
        @Override
        public void handle(RequestResources.ResourceFound event) {
           
            boolean check = false;
            synchronized(_non_idle_tasks)
            {
                Iterator i = _non_idle_tasks.iterator();
                while(i.hasNext())
                {
                    Task t = (Task)i.next();
                    if(t.getId() == event.getId())
                    {
                        long time = event.getTime_found_resource() - t.getStart_time();
                        t.setTime_to_find_resource(time);
                        check = true;
                        break;
                    }
                }
                if(!check)
                {
                    System.out.println("Weird");
                }
            }

        }
    };
    
    Handler<RequestResources.SetExpectedResponses> handleExpectedResponses = new Handler<RequestResources.SetExpectedResponses>() {
        @Override
        public void handle(RequestResources.SetExpectedResponses event) {
           
            synchronized(_idle_tasks)
            {
                Iterator i = _idle_tasks.iterator();
                while(i.hasNext())
                {
                    Task t = (Task)i.next();
                    if(t.getId() == event.getId())
                    {
                        t.setExpected_responses(event.getExpected_responses());
                        break;
                    }
                }
            }

        }
    };
    
    Handler<RequestResources.Ping> handlePing = new Handler<RequestResources.Ping>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResources.Ping event) {

            int _scheduler_task_id = event.getId();
            Address _scheduler = event.getSource();
            boolean queued = false;

            Task _task_to_look_for = null;

            synchronized (_tasks_runnning_on_this_machine) {
                Iterator i = _tasks_runnning_on_this_machine.iterator();
                while (i.hasNext()) {
                    Task t = (Task) i.next();
                    if (t.getId() == _scheduler_task_id && t.getScheduler().equals(_scheduler)) {
                        _task_to_look_for = t;
                        queued = false;
                        break;
                    }
                }
            }
            
            if(_task_to_look_for == null)
            {
                synchronized(_queue_for_this_machine)
                {
                    Iterator i = _queue_for_this_machine.iterator();
                    while(i.hasNext()){
                        Task t = (Task)i.next();
                        if(t.getId() == _scheduler_task_id && t.getScheduler().equals(_scheduler))
                        {
                            _task_to_look_for = t;
                            queued = true;
                            break;
                        }
                    }
                }
            }

            if (_task_to_look_for != null) {
                RequestResources.Pong p = new RequestResources.Pong(self, _task_to_look_for.getScheduler(), true, _task_to_look_for.getId());
                if(queued)
                {
                    p.setQueued(true);
                }
                trigger(p, networkPort);
            } else {
                RequestResources.Pong p = new RequestResources.Pong(self, _scheduler, false, _scheduler_task_id);
                trigger(p, networkPort);
            }

        }

   
    };

    Handler<RequestResources.Pong> handlePong = new Handler<RequestResources.Pong>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResources.Pong event) {

            int _task_id = event.getId();
            boolean _running = event.isRunning();

            synchronized (_non_idle_tasks) {
                Iterator i = _non_idle_tasks.iterator();
                while (i.hasNext()) {
                    Task t = (Task) i.next();
                    if (t.getId() == _task_id) {
                        t.setPonged(true);
                        t.setRunning(_running);
                        if(event.isQueued())
                        {
                            t.setQueued(true);
                        }
                        break;
                    }
                }
            }
        }

    };
    Handler<RequestResources.Confirm> handleConfirm = new Handler<RequestResources.Confirm>() {
        @Override
        public void handle(RequestResources.Confirm event) {

            int _cpu_to_allocate = event.getCpus();
            int _mem_to_allocate = event.getMem();
            int _task_time = event.getTask_time();
            int _schedulers_task_id = event.getId();

                Task _task_to_run = new Task(_schedulers_task_id, _cpu_to_allocate, _mem_to_allocate, _task_time, event.getSource());
                _task_to_run.setPotentialExecutor(self);

                if (availableResources.isAvailable(_task_to_run.getCpus(), _task_to_run.getMemory())) {
                    _tasks_runnning_on_this_machine.add(_task_to_run);
                    availableResources.allocate(_task_to_run.getCpus(), _task_to_run.getMemory());
                    trigger(new RequestResources.Pong(self, _task_to_run.getScheduler(), true, _task_to_run.getId()),networkPort);
                    trigger(new RequestResources.ResourceFound(self, _task_to_run.getScheduler(), _task_to_run.getId(), System.currentTimeMillis()),networkPort);
                    StartTimerForTaskToFinish(_task_to_run.getTask_time(), _task_to_run.getId(), _task_to_run.getScheduler());
                } else {
                    _queue_for_this_machine.add(_task_to_run);
                    trigger(new RequestResources.Pong(self, _task_to_run.getScheduler(), true, _task_to_run.getId()),networkPort);
                }
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
    
    Handler<TaskTimeOut> handleTaskTimeOut = new Handler<TaskTimeOut>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(TaskTimeOut event) {

            Address _scheduler = event.getScheduler();
            int _scheduler_task_id = event.getScheduler_task_id();
            Task _task_to_stop = null;

            synchronized (_tasks_runnning_on_this_machine) {
                Iterator i = _tasks_runnning_on_this_machine.iterator();
                while (i.hasNext()) {
                    Task t = (Task) i.next();
                    if (t.getId() == _scheduler_task_id && t.getScheduler().equals(_scheduler)) {
                        _task_to_stop = t;
                        break;
                    }
                }
            }
            if (_task_to_stop != null) {
                availableResources.release(_task_to_stop.getCpus(), _task_to_stop.getMemory());
                _tasks_runnning_on_this_machine.remove(_task_to_stop);
                            System.out.println(" ");
                            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                            System.out.println("Peer "+ self.getIp().getHostAddress()+" finished a task and released CPU:"+ _task_to_stop.getCpus()+" and MEM:"+_task_to_stop.getMemory());
                            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                            System.out.println(" ");

                if (_queue_for_this_machine.size() > 0) {
                    Task _task_top_of_queue = _queue_for_this_machine.get(0);
                    boolean _success = availableResources.isAvailable(_task_top_of_queue.getCpus(), _task_top_of_queue.getMemory());
                    
                    if (_success) {
                        trigger(new RequestResources.ResourceFound(self, _task_top_of_queue.getScheduler(), _task_top_of_queue.getId(), System.currentTimeMillis()),networkPort);
                        _tasks_runnning_on_this_machine.add(_task_top_of_queue);
                        _queue_for_this_machine.remove(_task_top_of_queue);
                        availableResources.allocate(_task_top_of_queue.getCpus(), _task_top_of_queue.getMemory());
                        StartTimerForTaskToFinish(_task_top_of_queue.getTask_time(), _task_top_of_queue.getId(), _task_top_of_queue.getScheduler());
                    }
                }
            } else {
                System.out.println("** ERROR ** task finished but was not found in the currently running tasks ** ERROR **");
            }

        }
    };

    Handler<RequestTimeout> handleRequestTimeOut = new Handler<RequestTimeout>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestTimeout event) {
            
            handleRequestResource.handle(new RequestResource(true));
        }
    };

    Handler<PongTimeOut> handlePongTimeOut = new Handler<PongTimeOut>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(PongTimeOut event) {

            int _task_id = event.getTask_id();

            synchronized (_non_idle_tasks) {
                Iterator i = _non_idle_tasks.iterator();
                while (i.hasNext()) {
                    Task t = (Task) i.next();
                    if (t.getId() == _task_id) {
                        if (t.isPonged()) {
                            if (t.isRunning()) {
                                t.setPonged(false);
                                RequestResources.Ping p = new RequestResources.Ping(self, t.getPotentialExecutor(), t.getId());
                                trigger(p, networkPort);
                                StartTimerForPongResponse(MAX_RESPONSE_TIMEOUT,t.getId());
                                break;
                            } else {
                                _non_idle_tasks.remove(t);
                                _finished_tasks.add(new FinishedTasks(t.getId(),t.getTime_to_find_resource(),t.getCpus(),t.getMemory(),self,t.getPotentialExecutor()));
                                break;
                            }
                        } else {
                            System.out.println(" ");
                            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                            System.out.println("Task with id ["+t.getId()+"] that was scheduled on peer [" + t.getPotentialExecutor().getIp().getHostAddress()+"] hasn't been ponged and is therefore rescheduled on another host using Resource Manager["+self.getIp().getHostAddress()+"]");
                            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                            System.out.println(" ");
                            _non_idle_tasks.remove(t);
                            t.setResponses(0);
                            t.setShortest_queue(Integer.MAX_VALUE);
                            _idle_tasks.add(t);
                            RequestResource r = new RequestResource(true);
                            handleRequestResource.handle(r);
                            break;
                        }
                    }
                }
            }

        }
    };

    Handler<RequestResources.Request> handleResourceAllocationRequest = new Handler<RequestResources.Request>() {
        @Override
        public void handle(RequestResources.Request event) {

            int _requested_cpu = event.getNumCpus();
            int _requested_mem = event.getAmountMemInMb();
            int _requested_task_id = event.getId();
            boolean _handle_allocation = availableResources.isAvailable(_requested_cpu, _requested_mem);
            
                RequestResources.Response r = new RequestResources.Response(self, event.getSource(), _handle_allocation, _requested_task_id, _queue_for_this_machine.size());
                if(event.isBatch_task()){
                    r.setBatch_task(event.isBatch_task());
                    r.setBatch_id(event.getBatch_id());
                }
                trigger(r, networkPort);
        }
    };
    
    Handler<RequestResources.FindBestPeerToRunTask> handleBestPeerRequest = new Handler<RequestResources.FindBestPeerToRunTask>() {
        @Override
        public void handle(RequestResources.FindBestPeerToRunTask event) {

            if(event.getType().equals("cpu"))
            {
                if(!_tmanCpu.isEmpty() && _tmanCpu.get(0).getAvailableResources().getNumFreeCpus() > availableResources.getNumFreeCpus())
                {
                    RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self, _tmanCpu.get(0).getAddress(), "cpu", event.getNumCpus(), event.getAmountMemInMb(), event.getId());
                    if(event.isBatch_task())
                    {
                        s.setBatch_task(true);
                        s.setBatch_id(event.getBatch_id());
                    }
                    s.setScheduler(event.getScheduler());
                    trigger (s,networkPort);
                }
                else
                {
                    RequestResources.SetExpectedResponses s = new RequestResources.SetExpectedResponses(self,event.getScheduler(),event.getId(),_tmanCpu.size());
                    trigger(s,networkPort);
                    
                    if(availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb()))
                    {
                        RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                        if(event.isBatch_task())
                        {
                            r.setBatch_task(true);
                            r.setBatch_id(event.getBatch_id());
                        }
                        trigger(r,networkPort);
                    }
                    else
                    {
                        if(!_tmanCpu.isEmpty())
                        {
                            for(PeerDescriptor pd : _tmanCpu)
                            {
                                RequestResources.Request r = new RequestResources.Request(event.getScheduler(), pd.getAddress(), event.getNumCpus(), event.getAmountMemInMb(), event.getId());
                                if(event.isBatch_task())
                                {
                                    r.setBatch_task(true);
                                    r.setBatch_id(event.getBatch_id());
                                }
                                trigger(r,networkPort);
                            }
                        }
                        else
                        {
                            RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                            if(event.isBatch_task())
                            {
                                r.setBatch_task(true);
                                r.setBatch_id(event.getBatch_id());
                            }
                            trigger(r,networkPort);
                        }
                    }
                }
            }
            else
            {
                if(!_tmanMem.isEmpty() && _tmanMem.get(0).getAvailableResources().getFreeMemInMbs() > availableResources.getFreeMemInMbs())
                {
                    RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self,_tmanMem.get(0).getAddress(),"mem",event.getNumCpus(),event.getAmountMemInMb(),event.getId());
                    if(event.isBatch_task())
                    {
                        s.setBatch_task(true);
                        s.setBatch_id(event.getBatch_id());
                    }
                    s.setScheduler(event.getScheduler());
                    trigger(s,networkPort);
                }
                else
                {
                    RequestResources.SetExpectedResponses s = new RequestResources.SetExpectedResponses(self,event.getScheduler(),event.getId(),_tmanCpu.size());
                    trigger(s,networkPort);
                    
                    if(availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb()))
                    {
                        RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                        if(event.isBatch_task())
                        {
                            r.setBatch_task(true);
                            r.setBatch_id(event.getBatch_id());
                        }
                        trigger(r,networkPort);
                    }
                    else
                    {
                        if(!_tmanMem.isEmpty())
                        {
                            for(PeerDescriptor pd : _tmanMem)
                            {
                                RequestResources.Request r = new RequestResources.Request(event.getScheduler(), pd.getAddress(), event.getNumCpus(), event.getAmountMemInMb(), event.getId());
                                if(r.isBatch_task())
                                {
                                    r.setBatch_task(true);
                                    r.setBatch_id(event.getBatch_id());
                                }
                                trigger(r,networkPort);
                            }
                        }
                        else
                        {
                            RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                            if(event.isBatch_task())
                            {
                                r.setBatch_task(true);
                                r.setBatch_id(event.getBatch_id());
                            }
                            trigger(r,networkPort);
                        }
                    }   
                }
            }
        }
    };
    Handler<RequestResources.Response> handleResourceAllocationResponse = new Handler<RequestResources.Response>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResources.Response event) {

            Task _task_in_question = null;
            synchronized (_idle_tasks) {
                Iterator i = _idle_tasks.iterator();
                while (i.hasNext()) {
                    Task t = (Task) i.next();
                    if(t.isBatch_task())
                    {
                        synchronized(_non_idle_tasks)
                        {
                            Iterator j = _non_idle_tasks.iterator();
                            while(j.hasNext())
                            {
                                Task te = (Task)j.next();
                                if(te.isBatch_task() && te.getBatch_id() == t.getBatch_id() && te.getPotentialExecutor().equals(event.getSource()))
                                {
                                    //Already scheduled a task from this batch on that machine
                                    return;
                                }
                            }
                        }
                        if (t.getId() == event.getId())
                        {
                            _task_in_question = t;
                            break;
                        }
                    }
                    else
                    {
                        if (t.getId() == event.getId()) {
                        _task_in_question = t;
                        break;
                        }
                    }
                    
                }
            }
            if (event.isSuccess()  && _task_in_question != null) {
                _idle_tasks.remove(_task_in_question);
                _task_in_question.setPotentialExecutor(event.getSource());
                _task_in_question.setRunning(true);
                _non_idle_tasks.add(_task_in_question);
                RequestResources.Confirm c = new RequestResources.Confirm(self, _task_in_question.getPotentialExecutor(), _task_in_question.getCpus(), _task_in_question.getMemory(), _task_in_question.getTask_time(), _task_in_question.getId());
                trigger(c, networkPort);
                StartTimerForPongResponse(MAX_RESPONSE_TIMEOUT, _task_in_question.getId());
            } else if (!event.isSuccess() && _task_in_question != null) {
                synchronized (_lock_responses) {
                    if (_task_in_question.getExpected_responses() - 1 == _task_in_question.getResponses()) // last responder
                    {
                        if (_task_in_question.getShortest_queue() > event.getQueue_size())
                        {
                            _task_in_question.setShortest_queue(event.getQueue_size());
                            _task_in_question.setShorest_queue_peer(event.getSource());
                        }
                            _idle_tasks.remove(_task_in_question);
                            _task_in_question.setPotentialExecutor(_task_in_question.getShorest_queue_peer());
                            _task_in_question.setRunning(true);
                            _non_idle_tasks.add(_task_in_question);
                            RequestResources.Confirm c = new RequestResources.Confirm(self, _task_in_question.getPotentialExecutor(), _task_in_question.getCpus(), _task_in_question.getMemory(), _task_in_question.getTask_time(), _task_in_question.getId());
                            trigger(c, networkPort);
                            StartTimerForPongResponse(MAX_RESPONSE_TIMEOUT, _task_in_question.getId());
                        
                    } else {
                        _task_in_question.setResponses(_task_in_question.getResponses() + 1);
                        if (_task_in_question.getShortest_queue() > event.getQueue_size()) {
                            _task_in_question.setShortest_queue(event.getQueue_size());
                            _task_in_question.setShorest_queue_peer(event.getSource());
                        }
                    }
                }
            }
        }
    };
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            
            ArrayList<Address> _addr = new ArrayList<Address>();
            for(PeerDescriptor pd : event.getSample())
            {
                _addr.add(pd.getAddress());
            }
            if(!_gradient)
            {
            System.out.println(self.getIp().getHostAddress() + " Received samples: " + _addr.size() + " and has a queue of size[" + _queue_for_this_machine.size() + "]");
            }
            // receive a new list of neighbours
            neighbours.clear();
            neighbours.addAll(_addr);

            // update routing tables
            for (Address p : neighbours) {
                int partition = p.getId() % configuration.getNumPartitions();
                List<PeerDescriptor> nodes = routingTable.get(partition);
                if (nodes == null) {
                    nodes = new ArrayList<PeerDescriptor>();
                    routingTable.put(partition, nodes);
                }
                // Note - this might replace an existing entry in Lucene
                nodes.add(new PeerDescriptor(p));
                // keep the freshest descriptors in this partition
                Collections.sort(nodes, peerAgeComparator);
                List<PeerDescriptor> nodesToRemove = new ArrayList<PeerDescriptor>();
                for (int i = nodes.size(); i > configuration.getMaxNumRoutingEntries(); i--) {
                    nodesToRemove.add(nodes.get(i - 1));
                }
                nodes.removeAll(nodesToRemove);
            }
        }
    };

    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResource event) {
            
            if(!event.isEmpty() && !event.isBatch_task())
            {
                System.out.println(" ");
                System.out.println("#################################################################################################################################################");
                System.out.println("ResourceManager [" + self.getIp().getHostAddress() + "] wants to schedule CPU[" + event.getNumCpus() + "] and MEM[" + event.getMemoryInMbs() + "]");
                System.out.println("#################################################################################################################################################");
                System.out.println(" ");
            }
            else if(event.isEmpty())
            {
                System.out.println(" ");
                System.out.println("#################################################################################################################################################");
                System.out.println("ResourceManager [" + self.getIp().getHostAddress() + "] wants to schedule task that did not finish, either due to a peer dying or 0 samples");
                System.out.println("#################################################################################################################################################");
                System.out.println(" ");
            }
            Task _task = null;
            int _cpu;
            int _mem;
            int _task_id;
            int _task_time;
            if(!event.isEmpty())
            {
                _cpu = event.getNumCpus();
                _mem = event.getMemoryInMbs();
                _task_id = getCurrentTaskId();
                _task_time = event.getTimeToHoldResource();
                
                if(event.isBatch_task())
                {
                    _task = new Task(_task_id, _cpu, _mem, _task_time, self);
                    _task.setBatch_task(true);
                    _task.setBatch_id(event.getBatch_id());
                }
                else
                {
                    _task = new Task(_task_id, _cpu, _mem, _task_time, self);
                    _task.setBatch_task(false);
                }
                if(!_idle_tasks.contains(_task))
                {
                    _idle_tasks.add(_task);
                }
            }
            if(!_gradient)
            {

                ArrayList<Address> _peers_to_probe = new ArrayList<Address>(neighbours);
            
                int _check_size = _peers_to_probe.size() - PROBES;

                for (int iterator = 0; iterator < _check_size; iterator++) {
                _peers_to_probe.remove(random.nextInt(_peers_to_probe.size()));
                }
                
                
                if(!_peers_to_probe.isEmpty())
                {
                    for (Address peer : _peers_to_probe) {
                        synchronized (_idle_tasks) {
                            Iterator i = _idle_tasks.iterator();
                            while (i.hasNext()) {
                                Task t = (Task) i.next();
                                t.setStart_time(System.currentTimeMillis());
                                t.setExpected_responses(_peers_to_probe.size());
                                RequestResources.Request r = new RequestResources.Request(self, peer, t.getCpus(), t.getMemory(), t.getId());
                                if(t.isBatch_task())
                                {
                                    r.setBatch_task(true);
                                    r.setBatch_id(t.getBatch_id());
                                }
                                trigger(r, networkPort);
                            }
                        }
                    }
                }
                else
                {
                    ScheduleTimeout st = new ScheduleTimeout(2000);
                    st.setTimeoutEvent(new RequestTimeout(st));
                    trigger(st,timerPort);
                }
            }
            else
            {
               for(Task task : _idle_tasks)
               {
                    String _type = DetermineTopology(task.getCpus(),task.getMemory());
                    if(_type.equals("cpu"))
                    {
                        if(!_tmanCpu.isEmpty())
                        {
                            RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self,_tmanCpu.get(0).getAddress(), "cpu", task.getCpus(), task.getMemory(), task.getId());
                            if(task.isBatch_task())
                            {
                                s.setBatch_task(true);
                                s.setBatch_id(task.getBatch_id());
                            }
                            task.setStart_time(System.currentTimeMillis());
                            s.setScheduler(self);
                            trigger(s,networkPort);
                        }
                        else
                        {
                            ScheduleTimeout st = new ScheduleTimeout(2000);
                            st.setTimeoutEvent(new RequestTimeout(st));
                            trigger(st,timerPort);
                        }
                    }
                    else
                    {
                        if(!_tmanMem.isEmpty())
                        {
                            RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self,_tmanMem.get(0).getAddress(), "mem", task.getCpus(), task.getMemory(), task.getId());
                            if(task.isBatch_task())
                            {
                                s.setBatch_task(true);
                                s.setBatch_id(task.getBatch_id());
                            }
                            task.setStart_time(System.currentTimeMillis());
                            s.setScheduler(self);
                            trigger(s,networkPort);
                        }
                        else
                        {
                            ScheduleTimeout st = new ScheduleTimeout(2000);
                            st.setTimeoutEvent(new RequestTimeout(st));
                            trigger(st,timerPort);
                        }
                    }
                }
            }
        }
    };
    
    Handler<BatchRequest> handleBatchRequest = new Handler<BatchRequest>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(BatchRequest event) {

            int _requested_machines = event.getMachines();
            int _requested_cpu = event.getNumCpus();
            int _requested_mem = event.getMemoryInMbs();
            
            int _batch_id = getCurrentBatchId();
            
            System.out.println(" ");
            System.out.println("#################################################################################################################################################");
            System.out.println("ResourceManager " + self.getIp().getHostAddress() + " wants a batchjob for " + _requested_machines + " machines, each with CPU["+_requested_cpu+"] and MEM["+_requested_mem+"]");
            System.out.println("#################################################################################################################################################");
            System.out.println(" ");
            
            for(int i = 0; i < _requested_machines;i++)
            {
                RequestResource r = new RequestResource(self.getId(),_requested_cpu,_requested_mem,event.getTimeToHoldResource());
                r.setBatch_task(true);
                r.setBatchId(_batch_id);
                handleRequestResource.handle(r);
            }
        }
    };
    
    Handler<TManSample.Cpu> handleTManSampleCpu = new Handler<TManSample.Cpu>() {
        @Override
        public void handle(TManSample.Cpu event) 
        {
            ArrayList<PeerDescriptor> sample = event.getSample();
            if(_gradient)
            {
                System.out.println(self.getIp().getHostAddress() + " Received cpu_samples: " + sample.size() + " and has a queue of size[" + _queue_for_this_machine.size() + "]");
            }
            _tmanCpu.clear();
            _tmanCpu.addAll(sample);
        }
    };
    
    Handler<TManSample.Mem> handleTManSampleMem = new Handler<TManSample.Mem>() {
        @Override
        public void handle(TManSample.Mem event) 
        {
            ArrayList<PeerDescriptor> sample = event.getSample();
            if(_gradient)
            {
            System.out.println(self.getIp().getHostAddress() + " Received mem_samples: " + sample.size() + " and has a queue of size[" + _queue_for_this_machine.size() + "]");
            }
            _tmanMem.clear();
            _tmanMem.addAll(sample);
        }
    };

    private void StartTimerForPongResponse(int delay, int id) {

        ScheduleTimeout st = new ScheduleTimeout(delay);
        PongTimeOut timeout = new PongTimeOut(st, id);
        st.setTimeoutEvent(timeout);
        trigger(st, timerPort);

    }

    private void StartTimerForTaskToFinish(int task_time, int scheduler_task_id, Address scheduler) {

        ScheduleTimeout st = new ScheduleTimeout(task_time);
        TaskTimeOut timeout = new TaskTimeOut(st, scheduler_task_id, scheduler);
        st.setTimeoutEvent(timeout);
        trigger(st, timerPort);
    }
    
     private String DetermineTopology(int _cpu, int _mem)
     {
         double mem = _mem / 1000;
         
         if(mem > _cpu)
         {
             return "mem";
         }
         else
         {
             return "cpu";
         }
     }

}
