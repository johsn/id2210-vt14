package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.BatchRequest;
import common.simulation.PrintAverage;
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
import java.util.concurrent.ConcurrentHashMap;
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
    List<PeerDescriptor> _tmanCpu = new ArrayList<PeerDescriptor>();
    List<PeerDescriptor> _tmanMem = new ArrayList<PeerDescriptor>();
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

    private List<TaskInformation> _idle_tasks = Collections.synchronizedList(new ArrayList());
    private List<TaskInformation> _non_idle_tasks = Collections.synchronizedList(new ArrayList());

    private List<TaskInformation> _tasks_runnning_on_this_machine = Collections.synchronizedList(new ArrayList());
    private List<TaskInformation> _queue_for_this_machine = Collections.synchronizedList(new ArrayList());
    private ArrayList<FinishedTasks> _finished_tasks = new ArrayList<FinishedTasks>();
    private Map<Integer, ArrayList<Address>> _batch_jobs_scheduling_locations = new ConcurrentHashMap<Integer, ArrayList<Address>>();

    // simply gets a new taskid
    private int getCurrentTaskId() {
        synchronized (_lock_task_id) {
            int tmp = this._current_task_id;
            this._current_task_id = this._current_task_id + 1;
            return tmp;
        }
    }

    //Simply gets a new batchid
    private int getCurrentBatchId() {
        synchronized (_lock_bacth_id) {
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
        subscribe(handleBatchRequest, indexPort);
        subscribe(handlePrintAverage, indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleTaskTimeOut, timerPort);
        subscribe(handlePongTimeOut, timerPort);
        subscribe(handleRequestTimeOut, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handlePing, networkPort);
        subscribe(handlePong, networkPort);
        subscribe(handleConfirm, networkPort);
        subscribe(handleExpectedResponses, networkPort);
        subscribe(handleBestPeerRequest, networkPort);
        subscribe(handleResourceFound, networkPort);
        subscribe(handleBatchTaskNotRunningThere, networkPort);
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

    // A handler that updates the hashmap in so that future batch tasks that have this id can be scheduled on the event source.
    Handler<RequestResources.ThisBatchTaskIsNotrunningHereAnymore> handleBatchTaskNotRunningThere = new Handler<RequestResources.ThisBatchTaskIsNotrunningHereAnymore>() {
        @Override
        public void handle(RequestResources.ThisBatchTaskIsNotrunningHereAnymore event) {

            for (Address a : _batch_jobs_scheduling_locations.get(event.getBatchId())) {
                if (a.equals(event.getSource())) {
                    _batch_jobs_scheduling_locations.get(event.getBatchId()).remove(a);
                    break;
                }
            }
        }
    };

    //Prints the averages and 99th percentile of finding some resource, only prints for Resource Managers that have had enough tests (n>30) to avoid bad examples
    Handler<PrintAverage> handlePrintAverage = new Handler<PrintAverage>() {
        @Override
        public void handle(PrintAverage event) {

            long _sum = 0, _average = 0;
            long _99thpercentile = 0;
            Double _index;
            ArrayList<Long> _to_sort = new ArrayList<Long>();
            if (_finished_tasks.size() > 30) {
                for (FinishedTasks ft : _finished_tasks) {
                    _sum += ft.getTime_to_find_resources_for_this_task();
                    _to_sort.add(ft.getTime_to_find_resources_for_this_task());
                }
                _average = _sum / _finished_tasks.size();
                Collections.sort(_to_sort);
                _index = 0.99 * (_to_sort.size() - 1);
                if (_index == Math.floor(_index) && !Double.isInfinite(_index)) {
                    Integer i = _index.intValue();
                    _99thpercentile = _to_sort.get(i);
                } else {
                    Double d = Math.ceil(_index);
                    Integer i = d.intValue();
                    _99thpercentile = _to_sort.get(i);
                }
                System.out.println(" ");
                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                System.out.println("ResourceManager " + self.getIp().getHostAddress() + " averaged :" + _average + " and got " + _99thpercentile + " in 99th percentile for finding tasks in milliseconds, averaged calculated using n= " + _finished_tasks.size());
                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                System.out.println(" ");
            }
        }
    };

    // Takes an event that a Executor has generated. 
    // The executor generated this event when it found resources for a particular task. 
    // This method sets the time for finding the resource for a certain task id.
    Handler<RequestResources.ResourceFound> handleResourceFound = new Handler<RequestResources.ResourceFound>() {
        @Override
        public void handle(RequestResources.ResourceFound event) {

            synchronized (_non_idle_tasks) {
                Iterator i = _non_idle_tasks.iterator();
                while (i.hasNext()) {
                    TaskInformation t = (TaskInformation) i.next();
                    if (t.getId() == event.getId()) {
                        long time = event.getTime_found_resource() - t.getStart_time();
                        t.setTime_to_find_resource(time);
                        break;
                    }
                }
            }

        }
    };

    // Sets the expected responses for a certain task. 
    // This handler is used gradient when the best peer has neighbors but cannot handle the task it self. 
    // It must probe its neighbors and in order to wait for responses it needs to know how many respones(neighbors) it should wait for before determening the best one.
    Handler<RequestResources.SetExpectedResponses> handleExpectedResponses = new Handler<RequestResources.SetExpectedResponses>() {
        @Override
        public void handle(RequestResources.SetExpectedResponses event) {

            synchronized (_idle_tasks) {
                Iterator i = _idle_tasks.iterator();
                while (i.hasNext()) {
                    TaskInformation t = (TaskInformation) i.next();
                    if (t.getId() == event.getId()) {
                        t.setExpected_responses(event.getExpected_responses());
                        break;
                    }
                }
            }

        }
    };
    // Handles a ping from a scheduler. Checks to see if the taskid and Address combination is in either the queue or the list of running tasks.
    // If it is found in either of the lists, it is considered running and a pong is created and sent as a response.
    // If it is not found it is considered done and a pong is created as a response.
    // The queued variable is just there for debugging purposes.
    Handler<RequestResources.Ping> handlePing = new Handler<RequestResources.Ping>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResources.Ping event) {

            int _scheduler_task_id = event.getId();
            Address _scheduler = event.getSource();
            boolean queued = false;

            TaskInformation _task_to_look_for = null;

            synchronized (_tasks_runnning_on_this_machine) {
                Iterator i = _tasks_runnning_on_this_machine.iterator();
                while (i.hasNext()) {
                    TaskInformation t = (TaskInformation) i.next();
                    if (t.getId() == _scheduler_task_id && t.getScheduler().equals(_scheduler)) {
                        _task_to_look_for = t;
                        queued = false;
                        break;
                    }
                }
            }

            if (_task_to_look_for == null) {
                synchronized (_queue_for_this_machine) {
                    Iterator i = _queue_for_this_machine.iterator();
                    while (i.hasNext()) {
                        TaskInformation t = (TaskInformation) i.next();
                        if (t.getId() == _scheduler_task_id && t.getScheduler().equals(_scheduler)) {
                            _task_to_look_for = t;
                            queued = true;
                            break;
                        }
                    }
                }
            }

            if (_task_to_look_for != null) {
                RequestResources.Pong p = new RequestResources.Pong(self, _task_to_look_for.getScheduler(), true, _task_to_look_for.getId());
                if (queued) {
                    p.setQueued(true);
                }
                trigger(p, networkPort);
            } else {
                RequestResources.Pong p = new RequestResources.Pong(self, _scheduler, false, _scheduler_task_id);
                trigger(p, networkPort);
            }

        }

    };

    // Handles a pong event from a executor.
    // Evaluates the event and sets the task indicated by the id to the values indicated by the event.
    // Again, the queue variable is only there for debugging purposes.
    Handler<RequestResources.Pong> handlePong = new Handler<RequestResources.Pong>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResources.Pong event) {

            int _task_id = event.getId();
            boolean _running = event.isRunning();

            synchronized (_non_idle_tasks) {
                Iterator i = _non_idle_tasks.iterator();
                while (i.hasNext()) {
                    TaskInformation t = (TaskInformation) i.next();
                    if (t.getId() == _task_id) {
                        t.setPonged(true);
                        t.setRunning(_running);
                        if (event.isQueued()) {
                            t.setQueued(true);
                        }
                        break;
                    }
                }
            }
        }

    };

    // Handles a confirm from a scheduler. This event indicates that the scheduler wants to place a task here.
    // The handler evaluates if the task indicated by the event can run right now or if it needs to be queued.
    // If it can run right away then a RequestFound event is generated as the task finally find resources.
    // Wether or not the task has to be queued a pong is generated indicating that the task is infact running and that the peer is alive.
    Handler<RequestResources.Confirm> handleConfirm = new Handler<RequestResources.Confirm>() {
        @Override
        public void handle(RequestResources.Confirm event) {

            int _cpu_to_allocate = event.getCpus();
            int _mem_to_allocate = event.getMem();
            int _task_time = event.getTask_time();
            int _schedulers_task_id = event.getId();

            TaskInformation _task_to_run = new TaskInformation(_schedulers_task_id, _cpu_to_allocate, _mem_to_allocate, _task_time, event.getSource());
            _task_to_run.setPotentialExecutor(self);
            if (event.isBatch_task()) {
                _task_to_run.setBatch_task(true);
                _task_to_run.setBatch_id(event.getBatch_id());
            }

            if (availableResources.isAvailable(_task_to_run.getCpus(), _task_to_run.getMemory())) {
                _tasks_runnning_on_this_machine.add(_task_to_run);
                availableResources.allocate(_task_to_run.getCpus(), _task_to_run.getMemory());
                System.out.println(" ");
                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                System.out.println("Task started to run at " + self.getIp().getHostAddress());
                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                System.out.println(" ");
                trigger(new RequestResources.Pong(self, _task_to_run.getScheduler(), true, _task_to_run.getId()), networkPort);
                trigger(new RequestResources.ResourceFound(self, _task_to_run.getScheduler(), _task_to_run.getId(), System.currentTimeMillis()), networkPort);
                StartTimerForTaskToFinish(_task_to_run.getTask_time(), _task_to_run.getId(), _task_to_run.getScheduler());
            } else {
                _queue_for_this_machine.add(_task_to_run);
                trigger(new RequestResources.Pong(self, _task_to_run.getScheduler(), true, _task_to_run.getId()), networkPort);
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

    // Handles tasktimeouts i.e it triggers when a task is done running.
    // The handler uses the event variables to identify what task that should be removed from the running list aswell as released.
    // It then checks to see if there is a task in the queue that could be run.
    // If the top task of the queue can run, it is allocated and a ResourceFound event is triggered to the scheduler of the task. This indicates that the task found its resources.
    Handler<TaskTimeOut> handleTaskTimeOut = new Handler<TaskTimeOut>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(TaskTimeOut event) {

            Address _scheduler = event.getScheduler();
            int _scheduler_task_id = event.getScheduler_task_id();
            TaskInformation _task_to_stop = null;

            synchronized (_tasks_runnning_on_this_machine) {
                Iterator i = _tasks_runnning_on_this_machine.iterator();
                while (i.hasNext()) {
                    TaskInformation t = (TaskInformation) i.next();
                    if (t.getId() == _scheduler_task_id && t.getScheduler().equals(_scheduler)) {
                        _task_to_stop = t;
                        break;
                    }
                }
            }
            if (_task_to_stop != null) {
                availableResources.release(_task_to_stop.getCpus(), _task_to_stop.getMemory());
                _tasks_runnning_on_this_machine.remove(_task_to_stop);
                if (_task_to_stop.isBatch_task()) {
                    // make sure to tell the scheduler that this batch-id is not running here anymore
                    trigger(new RequestResources.ThisBatchTaskIsNotrunningHereAnymore(self, _task_to_stop.getScheduler(), _task_to_stop.getBatch_id()), networkPort);
                }
                System.out.println(" ");
                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                System.out.println("Peer " + self.getIp().getHostAddress() + " finished a task for " + _task_to_stop.getScheduler().getIp().getHostAddress() + " and released CPU:" + _task_to_stop.getCpus() + " and MEM:" + _task_to_stop.getMemory());
                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                System.out.println(" ");

                if (_queue_for_this_machine.size() > 0) {
                    TaskInformation _task_top_of_queue = _queue_for_this_machine.get(0);
                    boolean _success = availableResources.isAvailable(_task_top_of_queue.getCpus(), _task_top_of_queue.getMemory());

                    if (_success) {
                        trigger(new RequestResources.ResourceFound(self, _task_top_of_queue.getScheduler(), _task_top_of_queue.getId(), System.currentTimeMillis()), networkPort);
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

    //This handler simply creates a empty request-resources event and runs it. It is used when there are 0 peers to probe / search for, i.e the task created is not scheduled.
    Handler<RequestTimeout> handleRequestTimeOut = new Handler<RequestTimeout>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestTimeout event) {

            handleRequestResource.handle(new RequestResource(true));
        }
    };

    // Handles pong timeouts. This timeout checks to see if the task indicated by the event has been ponged, i.e that the peer running it is alive.
    // It also checks if the task is still running. If it is ponged and running a new ping is triggered to the executor. If it is ponged and not running it is considered finished and added to the _finished_list.
    // If the task is not ponged, the executor is considered dead and the task is put in the list represting non scheduled tasks so that it can be scheduled in the future.
    // Also an empty reqest-resources-event is triggerd because there might not be any request-event to run the "dead task".
    Handler<PongTimeOut> handlePongTimeOut = new Handler<PongTimeOut>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(PongTimeOut event) {

            int _task_id = event.getTask_id();

            synchronized (_non_idle_tasks) {
                Iterator i = _non_idle_tasks.iterator();
                while (i.hasNext()) {
                    TaskInformation t = (TaskInformation) i.next();
                    if (t.getId() == _task_id) {
                        if (t.isPonged()) {
                            if (t.isRunning()) {
                                t.setPonged(false);
                                RequestResources.Ping p = new RequestResources.Ping(self, t.getPotentialExecutor(), t.getId());
                                trigger(p, networkPort);
                                StartTimerForPongResponse(MAX_RESPONSE_TIMEOUT, t.getId());
                                break;
                            } else {
                                _non_idle_tasks.remove(t);
                                _finished_tasks.add(new FinishedTasks(t.getId(), t.getTime_to_find_resource(), t.getCpus(), t.getMemory(), self, t.getPotentialExecutor()));
                                break;
                            }
                        } else {
                            System.out.println(" ");
                            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                            System.out.println("Task with id [" + t.getId() + "] that was scheduled on peer [" + t.getPotentialExecutor().getIp().getHostAddress() + "] hasn't been ponged and is therefore rescheduled on another host using Resource Manager[" + self.getIp().getHostAddress() + "]");
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

    // This handler handles requests from schedulers. It answers with a response indicating if the task could be run on this machine of not.
    // It also repsonds with its queues size to help the scheduler perform sparrow.
    Handler<RequestResources.Request> handleResourceAllocationRequest = new Handler<RequestResources.Request>() {
        @Override
        public void handle(RequestResources.Request event) {

            int _requested_cpu = event.getNumCpus();
            int _requested_mem = event.getAmountMemInMb();
            int _requested_task_id = event.getId();
            boolean _handle_allocation = availableResources.isAvailable(_requested_cpu, _requested_mem);

            RequestResources.Response r = new RequestResources.Response(self, event.getSource(), _handle_allocation, _requested_task_id, _queue_for_this_machine.size());
            if (event.isBatch_task()) {
                r.setBatch_task(event.isBatch_task());
                r.setBatch_id(event.getBatch_id());
            }
            trigger(r, networkPort);
        }
    };

    // This handler uses the tman sample to find the best peer for the type of request it is cpu/memory.
    // It keeps creating new FindBestPeerToRunTask events as long as the best peer in its sample has better resources available for the type.
    // When it eventully finds the best peer it checks if it can run the task. If it can a simple response is sent to the original scheduler.
    // If the best peer cannot handle the task, it sets requested responses and performs sparrow on its neighors, using the orignal scheduler as source.
    // If there are zero available neighbors then it responds to the original scheduler that it can be scheduled on itself, which will results in a queued task.
    // There is a possibility that the event recieves a list where it should not schedule tasks. This list represents addresses where the current batchid has already been scheduled
    Handler<RequestResources.FindBestPeerToRunTask> handleBestPeerRequest = new Handler<RequestResources.FindBestPeerToRunTask>() {
        @Override
        public void handle(RequestResources.FindBestPeerToRunTask event) {

            if (event.getType().equals("cpu")) {
                if (!event.getDont_schedule_here().isEmpty()) {
                    Iterator i = _tmanCpu.iterator();
                    while (i.hasNext()) {
                        PeerDescriptor pd = (PeerDescriptor) i.next();
                        if (event.getDont_schedule_here().contains(pd.getAddress())) {
                            i.remove();
                        }

                    }
                }
                if (!_tmanCpu.isEmpty() && _tmanCpu.get(0).getAvailableResources().getNumFreeCpus() > availableResources.getNumFreeCpus()) {
                    RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self, _tmanCpu.get(0).getAddress(), "cpu", event.getNumCpus(), event.getAmountMemInMb(), event.getId());
                    if (event.isBatch_task()) {
                        s.setBatch_task(true);
                        s.setBatch_id(event.getBatch_id());
                    }
                    if (!event.getDont_schedule_here().isEmpty()) {
                        s.getDont_schedule_here().addAll(event.getDont_schedule_here());
                    }
                    s.setScheduler(event.getScheduler());
                    trigger(s, networkPort);
                } else {
                    RequestResources.SetExpectedResponses s = new RequestResources.SetExpectedResponses(self, event.getScheduler(), event.getId(), _tmanCpu.size());
                    trigger(s, networkPort);

                    if (availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb())) {
                        RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                        if (event.isBatch_task()) {
                            r.setBatch_task(true);
                            r.setBatch_id(event.getBatch_id());
                        }
                        trigger(r, networkPort);
                    } else {
                        if (!_tmanCpu.isEmpty()) {
                            for (PeerDescriptor pd : _tmanCpu) {
                                RequestResources.Request r = new RequestResources.Request(event.getScheduler(), pd.getAddress(), event.getNumCpus(), event.getAmountMemInMb(), event.getId());
                                if (event.isBatch_task()) {
                                    r.setBatch_task(true);
                                    r.setBatch_id(event.getBatch_id());
                                }
                                trigger(r, networkPort);
                            }
                        } else {
                            RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                            if (event.isBatch_task()) {
                                r.setBatch_task(true);
                                r.setBatch_id(event.getBatch_id());
                            }
                            trigger(r, networkPort);
                        }
                    }
                }
            } else {
                if (!event.getDont_schedule_here().isEmpty()) {
                    Iterator k = _tmanMem.iterator();
                    while (k.hasNext()) {
                        PeerDescriptor pd = (PeerDescriptor) k.next();
                        if (event.getDont_schedule_here().contains(pd.getAddress())) {
                            k.remove();
                        }

                    }
                }
                if (!_tmanMem.isEmpty() && _tmanMem.get(0).getAvailableResources().getFreeMemInMbs() > availableResources.getFreeMemInMbs()) {
                    RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self, _tmanMem.get(0).getAddress(), "mem", event.getNumCpus(), event.getAmountMemInMb(), event.getId());
                    if (event.isBatch_task()) {
                        s.setBatch_task(true);
                        s.setBatch_id(event.getBatch_id());
                    }
                    if (!event.getDont_schedule_here().isEmpty()) {
                        s.getDont_schedule_here().addAll(event.getDont_schedule_here());
                    }
                    s.setScheduler(event.getScheduler());
                    trigger(s, networkPort);
                } else {
                    RequestResources.SetExpectedResponses s = new RequestResources.SetExpectedResponses(self, event.getScheduler(), event.getId(), _tmanCpu.size());
                    trigger(s, networkPort);

                    if (availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb())) {
                        RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                        if (event.isBatch_task()) {
                            r.setBatch_task(true);
                            r.setBatch_id(event.getBatch_id());
                        }
                        trigger(r, networkPort);
                    } else {
                        if (!_tmanMem.isEmpty()) {
                            for (PeerDescriptor pd : _tmanMem) {
                                RequestResources.Request r = new RequestResources.Request(event.getScheduler(), pd.getAddress(), event.getNumCpus(), event.getAmountMemInMb(), event.getId());
                                if (r.isBatch_task()) {
                                    r.setBatch_task(true);
                                    r.setBatch_id(event.getBatch_id());
                                }
                                trigger(r, networkPort);
                            }
                        } else {
                            RequestResources.Response r = new RequestResources.Response(self, event.getScheduler(), true, event.getId(), _queue_for_this_machine.size());
                            if (event.isBatch_task()) {
                                r.setBatch_task(true);
                                r.setBatch_id(event.getBatch_id());
                            }
                            trigger(r, networkPort);
                        }
                    }
                }
            }
        }
    };
    // Handles respons events. Finds the task indicated by the id and evaluates its success.
    // If the task was request was succesful then a confirm is sent to the request-source. Also a hearbeating mechanism starts, i.e a ping is triggered.
    // If the request was not successful then the handler checks to see how many requests it has recieved. If this is the last request it sends the confirm to the 
    // best peer, i.e the shortest queue peer. If this is not the last peer it updates shortest queue information and shortest queue peer.
    // If the task is a batch task then handler makes sure to add this information to the hashmap so that future request-resource wont schedule batch-tasks with the same batchid on the same machine.
    Handler<RequestResources.Response> handleResourceAllocationResponse = new Handler<RequestResources.Response>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResources.Response event) {

            TaskInformation _task_in_question = null;
            synchronized (_idle_tasks) {
                Iterator i = _idle_tasks.iterator();
                while (i.hasNext()) {
                    TaskInformation t = (TaskInformation) i.next();
                    if (t.getId() == event.getId()) {
                        _task_in_question = t;
                        break;
                    }
                }
            }
            if (event.isSuccess() && _task_in_question != null) {
                _idle_tasks.remove(_task_in_question);
                _task_in_question.setPotentialExecutor(event.getSource());
                _task_in_question.setRunning(true);
                _non_idle_tasks.add(_task_in_question);
                if (_task_in_question.isBatch_task()) {
                    if (_batch_jobs_scheduling_locations.get(_task_in_question.getBatch_id()) != null) {
                        _batch_jobs_scheduling_locations.get(_task_in_question.getBatch_id()).add(_task_in_question.getPotentialExecutor());
                    } else {
                        ArrayList<Address> _new_entry = new ArrayList<Address>();
                        _new_entry.add(_task_in_question.getPotentialExecutor());
                        _batch_jobs_scheduling_locations.put(_task_in_question.getBatch_id(), _new_entry);
                    }
                }
                RequestResources.Confirm c = new RequestResources.Confirm(self, _task_in_question.getPotentialExecutor(), _task_in_question.getCpus(), _task_in_question.getMemory(), _task_in_question.getTask_time(), _task_in_question.getId());
                if (_task_in_question.isBatch_task()) {
                    c.setBatch_task(true);
                    c.setBatch_id(_task_in_question.getBatch_id());
                }
                trigger(c, networkPort);
                StartTimerForPongResponse(MAX_RESPONSE_TIMEOUT, _task_in_question.getId());
            } else if (!event.isSuccess() && _task_in_question != null) {
                synchronized (_lock_responses) {
                    if (_task_in_question.getExpected_responses() - 1 == _task_in_question.getResponses()) // last responder
                    {
                        if (_task_in_question.getShortest_queue() > event.getQueue_size()) {
                            _task_in_question.setShortest_queue(event.getQueue_size());
                            _task_in_question.setShorest_queue_peer(event.getSource());
                        }
                        _idle_tasks.remove(_task_in_question);
                        _task_in_question.setPotentialExecutor(_task_in_question.getShorest_queue_peer());
                        _task_in_question.setRunning(true);
                        _non_idle_tasks.add(_task_in_question);
                        if (_task_in_question.isBatch_task()) {
                            if (_batch_jobs_scheduling_locations.get(_task_in_question.getBatch_id()) != null) {
                                _batch_jobs_scheduling_locations.get(_task_in_question.getBatch_id()).add(_task_in_question.getPotentialExecutor());
                            } else {
                                ArrayList<Address> _new_entry = new ArrayList<Address>();
                                _new_entry.add(_task_in_question.getPotentialExecutor());
                                _batch_jobs_scheduling_locations.put(_task_in_question.getBatch_id(), _new_entry);
                            }
                        }
                        RequestResources.Confirm c = new RequestResources.Confirm(self, _task_in_question.getPotentialExecutor(), _task_in_question.getCpus(), _task_in_question.getMemory(), _task_in_question.getTask_time(), _task_in_question.getId());
                        if (_task_in_question.isBatch_task()) {
                            c.setBatch_task(true);
                            c.setBatch_id(_task_in_question.getBatch_id());
                        }
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
            for (PeerDescriptor pd : event.getSample()) {
                _addr.add(pd.getAddress());
            }
            if (!_gradient) {
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

    // Handles request-resource events. Creates a TaskInformation from the event variables and puts it into _idle_tasks which is a list that represents unscheduled tasks.
    // Then it depends on wether _gradient is true or false. If false then cyclonsample is used and PROBES decides how many peers are requested.
    // If _gradient is true, then a search for the best possible peer is started.
    // If there are no samples in either tman or cyclon, then a requesttimeout is generated.
    // Also if the task to schedule is a batch task, the hashmap is evaluated in order to remove addresses that we cannot schedule batch tasks at.
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        @SuppressWarnings("SynchronizeOnNonFinalField")
        public void handle(RequestResource event) {

            if (!event.isEmpty() && !event.isBatch_task()) {
                System.out.println(" ");
                System.out.println("#################################################################################################################################################");
                System.out.println("ResourceManager [" + self.getIp().getHostAddress() + "] wants to schedule CPU[" + event.getNumCpus() + "] and MEM[" + event.getMemoryInMbs() + "]");
                System.out.println("#################################################################################################################################################");
                System.out.println(" ");
            }
            TaskInformation _task = null;
            int _cpu;
            int _mem;
            int _task_id;
            int _task_time;
            if (!event.isEmpty()) {
                _cpu = event.getNumCpus();
                _mem = event.getMemoryInMbs();
                _task_id = getCurrentTaskId();
                _task_time = event.getTimeToHoldResource();

                if (event.isBatch_task()) {
                    _task = new TaskInformation(_task_id, _cpu, _mem, _task_time, self);
                    _task.setBatch_task(true);
                    _task.setBatch_id(event.getBatch_id());
                } else {
                    _task = new TaskInformation(_task_id, _cpu, _mem, _task_time, self);
                    _task.setBatch_task(false);
                }
                if (!_idle_tasks.contains(_task)) {
                    _idle_tasks.add(_task);
                }
            }
            if (!_gradient) {
                synchronized (_idle_tasks) {
                    Iterator i = _idle_tasks.iterator();
                    while (i.hasNext()) {
                        TaskInformation t = (TaskInformation) i.next();
                        ArrayList<Address> _peers_to_probe = new ArrayList<Address>(neighbours);
                        if (t.isBatch_task()) {
                            if (_batch_jobs_scheduling_locations.get(t.getBatch_id()) != null) {
                                ArrayList<Address> _peers_not_to_probe = _batch_jobs_scheduling_locations.get(t.getBatch_id());
                                _peers_to_probe.removeAll(_peers_not_to_probe);
                            }
                        }
                        while (_peers_to_probe.size() > PROBES) {
                            _peers_to_probe.remove(random.nextInt(_peers_to_probe.size()));
                        }
                        t.getPeers_to_probe().addAll(_peers_to_probe);

                        if (!t.getPeers_to_probe().isEmpty()) {
                            for (Address peer : t.getPeers_to_probe()) {
                                t.setStart_time(System.currentTimeMillis());
                                t.setExpected_responses(t.getPeers_to_probe().size());
                                RequestResources.Request r = new RequestResources.Request(self, peer, t.getCpus(), t.getMemory(), t.getId());
                                if (t.isBatch_task()) {
                                    r.setBatch_task(true);
                                    r.setBatch_id(t.getBatch_id());
                                }
                                trigger(r, networkPort);
                            }
                        } else {
                            StartTimerForRequestRestart(2000);
                        }
                    }
                }
            } else {
                synchronized (_idle_tasks) {
                    Iterator i = _idle_tasks.iterator();
                    while (i.hasNext()) {
                        TaskInformation task = (TaskInformation) i.next();
                        String _type = DetermineTopology(task.getCpus(), task.getMemory());
                        if (_type.equals("cpu")) {
                            if (!_tmanCpu.isEmpty()) {
                                ArrayList<PeerDescriptor> _peers_to_probe = new ArrayList<PeerDescriptor>(_tmanCpu);
                                ArrayList<Address> _peers_not_to_probe = new ArrayList<Address>();
                                if (task.isBatch_task()) {
                                    if (_batch_jobs_scheduling_locations.get(task.getBatch_id()) != null) {
                                        _peers_not_to_probe = _batch_jobs_scheduling_locations.get(task.getBatch_id());
                                    }
                                    if (!_peers_not_to_probe.isEmpty()) {
                                        Iterator j = _peers_to_probe.iterator();
                                        while (j.hasNext()) {
                                            PeerDescriptor pd = (PeerDescriptor) j.next();
                                            if (_peers_not_to_probe.contains(pd.getAddress())) {
                                                j.remove();
                                            }
                                        }
                                    }
                                }
                                if (!_peers_to_probe.isEmpty()) {
                                    RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self, _peers_to_probe.get(0).getAddress(), "cpu", task.getCpus(), task.getMemory(), task.getId());
                                    if (task.isBatch_task()) {
                                        s.setBatch_task(true);
                                        s.setBatch_id(task.getBatch_id());
                                    }
                                    if (!_peers_not_to_probe.isEmpty()) {
                                        s.getDont_schedule_here().addAll(_peers_not_to_probe);
                                    }
                                    task.setStart_time(System.currentTimeMillis());
                                    s.setScheduler(self);
                                    trigger(s, networkPort);
                                } else {
                                    StartTimerForRequestRestart(2000);
                                }
                            } else {
                                StartTimerForRequestRestart(2000);
                            }
                        } else {
                            if (!_tmanMem.isEmpty()) {
                                ArrayList<PeerDescriptor> _peers_to_probe = new ArrayList<PeerDescriptor>(_tmanMem);
                                ArrayList<Address> _peers_not_to_probe = new ArrayList<Address>();
                                if (task.isBatch_task()) {
                                    if (_batch_jobs_scheduling_locations.get(task.getBatch_id()) != null) {
                                        _peers_not_to_probe = _batch_jobs_scheduling_locations.get(task.getBatch_id());
                                    }
                                    if (!_peers_not_to_probe.isEmpty()) {
                                        Iterator k = _peers_to_probe.iterator();
                                        while (k.hasNext()) {
                                            PeerDescriptor pd = (PeerDescriptor) k.next();
                                            if (_peers_not_to_probe.contains(pd.getAddress())) {
                                                k.remove();
                                            }
                                        }
                                    }
                                }
                                if (!_peers_to_probe.isEmpty()) {
                                    RequestResources.FindBestPeerToRunTask s = new RequestResources.FindBestPeerToRunTask(self, _peers_to_probe.get(0).getAddress(), "mem", task.getCpus(), task.getMemory(), task.getId());
                                    if (task.isBatch_task()) {
                                        s.setBatch_task(true);
                                        s.setBatch_id(task.getBatch_id());
                                    }
                                    if (!_peers_not_to_probe.isEmpty()) {
                                        s.getDont_schedule_here().addAll(_peers_not_to_probe);
                                    }
                                    task.setStart_time(System.currentTimeMillis());
                                    s.setScheduler(self);
                                    trigger(s, networkPort);
                                } else {
                                    StartTimerForRequestRestart(2000);
                                }
                            } else {
                                StartTimerForRequestRestart(2000);
                            }
                        }
                    }
                }
            }
        }
    };

    // Handles batch-requests.
    // Creates a request-resource-event and gives it a batch-id aswell as a boolean indicating that its a batch-task.
    // Each task in the batch-request is givien the same batch-id so that these tasks must be scheduled on unique machines.
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
            System.out.println("ResourceManager " + self.getIp().getHostAddress() + " wants a batchjob for " + _requested_machines + " machines, each with CPU[" + _requested_cpu + "] and MEM[" + _requested_mem + "]");
            System.out.println("#################################################################################################################################################");
            System.out.println(" ");

            for (int i = 0; i < _requested_machines; i++) {
                RequestResource r = new RequestResource(self.getId(), _requested_cpu, _requested_mem, event.getTimeToHoldResource());
                r.setBatch_task(true);
                r.setBatchId(_batch_id);
                handleRequestResource.handle(r);
            }
        }
    };

    //Updates the cpu tman sample.
    Handler<TManSample.Cpu> handleTManSampleCpu = new Handler<TManSample.Cpu>() {
        @Override
        public void handle(TManSample.Cpu event) {
            ArrayList<PeerDescriptor> sample = event.getSample();
            if (_gradient) {
                System.out.println(self.getIp().getHostAddress() + " Received cpu_samples: " + sample.size() + " and has a queue of size[" + _queue_for_this_machine.size() + "]");
            }
            _tmanCpu.clear();
            _tmanCpu.addAll(sample);
            if (_tmanCpu.size() > 10) {
                System.exit(-1);
            }
        }
    };

    // Updates the memory tman sample
    Handler<TManSample.Mem> handleTManSampleMem = new Handler<TManSample.Mem>() {
        @Override
        public void handle(TManSample.Mem event) {
            ArrayList<PeerDescriptor> sample = event.getSample();
            if (_gradient) {
                System.out.println(self.getIp().getHostAddress() + " Received mem_samples: " + sample.size() + " and has a queue of size[" + _queue_for_this_machine.size() + "]");
            }
            _tmanMem.clear();
            _tmanMem.addAll(sample);
            if (_tmanMem.size() > 10) {
                System.exit(-1);
            }

        }
    };

    // Starts the timer for a pongtimeout
    private void StartTimerForRequestRestart(int delay) {

        ScheduleTimeout st = new ScheduleTimeout(delay);
        RequestTimeout rt = new RequestTimeout(st);
        st.setTimeoutEvent(rt);
        trigger(st, timerPort);

    }

    private void StartTimerForPongResponse(int delay, int id) {

        ScheduleTimeout st = new ScheduleTimeout(delay);
        PongTimeOut timeout = new PongTimeOut(st, id);
        st.setTimeoutEvent(timeout);
        trigger(st, timerPort);

    }

    // Starts the timer for a tasktimeout
    private void StartTimerForTaskToFinish(int task_time, int scheduler_task_id, Address scheduler) {

        ScheduleTimeout st = new ScheduleTimeout(task_time);
        TaskTimeOut timeout = new TaskTimeOut(st, scheduler_task_id, scheduler);
        st.setTimeoutEvent(timeout);
        trigger(st, timerPort);
    }

    // Determines what tmansample the request-resource event should use to search for best peer
    private String DetermineTopology(int _cpu, int _mem) {
        double mem = _mem / 1000;

        if (mem > _cpu) {
            return "mem";
        } else {
            return "cpu";
        }
    }

}
