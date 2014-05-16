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
    // When you partition the index you need to find new nodes
    // This is a routing table maintaining a list of pairs in each partition.
    private Map<Integer, List<PeerDescriptor>> routingTable;

    private static final int PROBES = 10;
    private static final int MAX_RESPONSE_TIMEOUT = 2000;
    private int _current_task_id = 0;
    private final Object _lock_task_id = new Object();
    private final Object _lock_responses = new Object();

    private List<Task> _idle_tasks = Collections.synchronizedList(new ArrayList());
    private List<Task> _non_idle_tasks = Collections.synchronizedList(new ArrayList());

    private List<Task> _tasks_runnning_on_this_machine = Collections.synchronizedList(new ArrayList());
    private List<Task> _queue_for_this_machine = Collections.synchronizedList(new ArrayList());

    private int getCurrentTaskId() {
        synchronized (_lock_task_id) {
            int tmp = this._current_task_id;
            this._current_task_id = this._current_task_id + 1;
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
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleTaskTimeOut, timerPort);
        subscribe(handlePongTimeOut, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handlePing, networkPort);
        subscribe(handlePong, networkPort);
        subscribe(handleConfirm, networkPort);
        subscribe(handleTManSample, tmanPort);
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

    Handler<RequestResources.Ping> handlePing = new Handler<RequestResources.Ping>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResources.Ping event) {

            int _scheduler_task_id = event.getId();
            Address _scheduler = event.getSource();

            Task _task_to_look_for = null;

            synchronized (_tasks_runnning_on_this_machine) {
                Iterator i = _tasks_runnning_on_this_machine.iterator();
                while (i.hasNext()) {
                    Task t = (Task) i.next();
                    if (t.getId() == _scheduler_task_id && t.getScheduler().equals(_scheduler)) {
                        _task_to_look_for = t;
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
                            break;
                        }
                    }
                }
            }

            if (_task_to_look_for != null) {
                RequestResources.Pong p = new RequestResources.Pong(self, _task_to_look_for.getScheduler(), true, _task_to_look_for.getId());
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
                StartTimerForTaskToFinish(_task_to_run.getTask_time(), _task_to_run.getId(), _task_to_run.getScheduler());
            } else {
                _queue_for_this_machine.add(_task_to_run);
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
                                break;
                            } else {
                                _non_idle_tasks.remove(t);
                                break;
                            }
                        } else {
                            System.out.println(" ");
                            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                            System.out.println("Peer "+ t.getPotentialExecutor().getIp().getHostAddress()+" hasn't ponged and is therefore presumed dead");
                            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                            System.out.println(" ");
                            _non_idle_tasks.remove(t);
                            _idle_tasks.add(t);
                            Address _executor = t.getPotentialExecutor();
                            synchronized(_non_idle_tasks)
                            {
                                Iterator j = _non_idle_tasks.iterator();
                                while(j.hasNext())
                                {
                                    Task task = (Task)j.next();
                                    if(task.getPotentialExecutor().equals(_executor))
                                    {
                                        _non_idle_tasks.remove(task);
                                        _idle_tasks.add(task);
                                    }
                                }
                            }
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
            trigger(r, networkPort);
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
                    if (t.getId() == event.getId()) {
                        _task_in_question = t;
                        break;
                    }
                }
            }
            if (event.isSuccess() && _task_in_question != null) {
                _idle_tasks.remove(_task_in_question);
                _task_in_question.setPotentialExecutor(event.getSource());
                _non_idle_tasks.add(_task_in_question);
                RequestResources.Confirm c = new RequestResources.Confirm(self, event.getSource(), _task_in_question.getCpus(), _task_in_question.getMemory(), _task_in_question.getTask_time(), _task_in_question.getId());
                trigger(c, networkPort);
                RequestResources.Ping p = new RequestResources.Ping(self, event.getSource(), _task_in_question.getId());
                trigger(p, networkPort);
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
                            _task_in_question.setPotentialExecutor(event.getSource());
                            _non_idle_tasks.add(_task_in_question);
                            RequestResources.Confirm c = new RequestResources.Confirm(self, _task_in_question.getShorest_queue_peer(), _task_in_question.getCpus(), _task_in_question.getMemory(), _task_in_question.getTask_time(), _task_in_question.getId());
                            trigger(c, networkPort);
                            RequestResources.Ping p = new RequestResources.Ping(self, _task_in_question.getShorest_queue_peer(), _task_in_question.getId());
                            trigger(p, networkPort);
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
            System.out.println(self.getIp().getHostAddress() + " Received samples: " + event.getSample().size() + " and has a queue of size[" + _queue_for_this_machine.size() + "]");

            // receive a new list of neighbours
            neighbours.clear();
            neighbours.addAll(event.getSample());

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

            System.out.println(" ");
            System.out.println("#################################################################################################################################################");
            System.out.println("ResourceManager [" + self.getIp().getHostAddress() + "] wants to schedule CPU[" + event.getNumCpus() + "] and MEM[" + event.getMemoryInMbs() + "]");
            System.out.println("#################################################################################################################################################");
            System.out.println(" ");
            int _cpu = event.getNumCpus();
            int _mem = event.getMemoryInMbs();
            int _task_id = getCurrentTaskId();
            int _task_time = event.getTimeToHoldResource();
            Task _task = new Task(_task_id, _cpu, _mem, _task_time, self);

            

            ArrayList<Address> _peers_to_probe = new ArrayList<Address>(neighbours);
            
            int _check_size = _peers_to_probe.size() - PROBES;

            for (int iterator = 0; iterator < _check_size; iterator++) {
                _peers_to_probe.remove(random.nextInt(_peers_to_probe.size()));
            }
            
            _idle_tasks.add(_task);

            for (Address peer : _peers_to_probe) {
                synchronized (_idle_tasks) {
                    Iterator i = _idle_tasks.iterator();
                    while (i.hasNext()) {
                        Task t = (Task) i.next();
                        t.setExpected_responses(_peers_to_probe.size());
                        RequestResources.Request r = new RequestResources.Request(self, peer, t.getCpus(), t.getMemory(), t.getId());
                        trigger(r, networkPort);
                    }
                }
            }

        }
    };
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            // TODO: 
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

}
