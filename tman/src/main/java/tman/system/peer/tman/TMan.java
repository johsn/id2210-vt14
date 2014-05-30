package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import java.util.ArrayList;

import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.DescriptorBuffer;
import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

public final class TMan extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(TMan.class);

    Negative<TManSamplePort> tmanPort = negative(TManSamplePort.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    private long period;
    private Address self;

    private ArrayList<PeerDescriptor> _view_cpu;
    private ArrayList<PeerDescriptor> _view_mem;
    private final Object _view_change = new Object();

    private TManConfiguration tmanConfiguration;
    private Random r;
    private AvailableResources availableResources;
    PeerDescriptor _mydescriptor;
    private UUID _id;
    private int _view_size = 10;

    public class TManSchedule extends Timeout {

        public TManSchedule(SchedulePeriodicTimeout request) {
            super(request);
        }

        public TManSchedule(ScheduleTimeout request) {
            super(request);
        }
    }

    public class TManPost extends Timeout {

        public TManPost(SchedulePeriodicTimeout request) {
            super(request);
        }

        public TManPost(ScheduleTimeout request) {
            super(request);
        }
    }

    public TMan() {

        _view_cpu = new ArrayList<PeerDescriptor>();
        _view_mem = new ArrayList<PeerDescriptor>();

        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleTManPost, timerPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManPartnersResponse, networkPort);
        subscribe(handleTManPartnersRequest, networkPort);
    }

    Handler<TManInit> handleInit = new Handler<TManInit>() {
        @Override
        public void handle(TManInit init) {
            self = init.getSelf();
            tmanConfiguration = init.getConfiguration();
            period = tmanConfiguration.getPeriod();
            r = new Random(tmanConfiguration.getSeed());
            availableResources = init.getAvailableResources();
            _mydescriptor = new PeerDescriptor(self);
            _mydescriptor.setAvailableResources(availableResources);
            _id = UUID.randomUUID();

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new TManSchedule(rst));
            trigger(rst, timerPort);

            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(period, period);
            spt.setTimeoutEvent(new TManPost(spt));
            trigger(spt, timerPort);

        }
    };

    // posts new tmansample each TManPost event
    Handler<TManPost> handleTManPost = new Handler<TManPost>() {
        @Override
        public void handle(TManPost event) {

            synchronized (_view_change) {
                TManSample.Cpu _sample_cpu = new TManSample.Cpu(selectView(rank(_view_cpu, "cpu")));
                TManSample.Mem _sample_mem = new TManSample.Mem(selectView(rank(_view_mem, "mem")));
                trigger(_sample_cpu, tmanPort);
                trigger(_sample_mem, tmanPort);
            }

        }
    };

    // Selects a peer to exchange descriptors with softmax default 1.0. 
    // Creates a DescriptorBuffer to exchange with the selected peer.
    // Start a request for exchange
    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {

            if (_view_cpu.size() > 0 && _view_mem.size() > 0) {
                ArrayList<PeerDescriptor> _buffer_cpu = new ArrayList<PeerDescriptor>();
                ArrayList<PeerDescriptor> _buffer_mem = new ArrayList<PeerDescriptor>();

                PeerDescriptor peer_cpu = getSoftMaxPeerDescriptor(_view_cpu, "cpu");
                PeerDescriptor peer_mem = getSoftMaxPeerDescriptor(_view_mem, "mem");

                ArrayList<PeerDescriptor> _mydescriptorlist = new ArrayList<PeerDescriptor>();
                _mydescriptorlist.add(_mydescriptor);

                _buffer_cpu = merge(_view_cpu, _mydescriptorlist);
                _buffer_mem = merge(_view_mem, _mydescriptorlist);

                DescriptorBuffer db_cpu = new DescriptorBuffer(_mydescriptor, _buffer_cpu);
                DescriptorBuffer db_mem = new DescriptorBuffer(_mydescriptor, _buffer_mem);

                ExchangeMsg.Request r_cpu = new ExchangeMsg.Request(_id, db_cpu, "cpu", self, peer_cpu.getAddress());
                ExchangeMsg.Request r_mem = new ExchangeMsg.Request(_id, db_mem, "mem", self, peer_mem.getAddress());

                trigger(r_cpu, networkPort);
                trigger(r_mem, networkPort);
            }
        }
    };

    // Merges cyclonsample with cpu and memory views.
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            List<PeerDescriptor> cyclonPartners = event.getSample();
            synchronized (_view_change) {
                _view_cpu = merge(_view_cpu, cyclonPartners);
                _view_mem = merge(_view_mem, cyclonPartners);
            }
        }

    };
    // Handles a request to exchange descriptorbuffer
    // Checks what type of request it is memory or cpu
    // Creates a response using its own descriptorbuffer
    // Then merges own view with recieved view and ranks it and selects the best peers for the final view.
    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {

            DescriptorBuffer _recieved_buffer = event.getRandomBuffer();
            ArrayList<PeerDescriptor> buffer;
            if (event.getType().equals("cpu")) {
                ArrayList<PeerDescriptor> _mydescriptorlist = new ArrayList<PeerDescriptor>();
                _mydescriptorlist.add(_mydescriptor);
                buffer = merge(_view_cpu, _mydescriptorlist);
                DescriptorBuffer db = new DescriptorBuffer(_mydescriptor, buffer);
                ExchangeMsg.Response r = new ExchangeMsg.Response(_id, db, "cpu", self, event.getSource());
                trigger(r, networkPort);
                buffer = merge(_view_cpu, _recieved_buffer.getDescriptors());
                synchronized (_view_change) {
                    _view_cpu = selectView(rank(buffer, event.getType()));
                }
            } else {
                ArrayList<PeerDescriptor> _mydescriptorlist = new ArrayList<PeerDescriptor>();
                _mydescriptorlist.add(_mydescriptor);
                buffer = merge(_view_mem, _mydescriptorlist);
                DescriptorBuffer db = new DescriptorBuffer(_mydescriptor, buffer);
                ExchangeMsg.Response r = new ExchangeMsg.Response(_id, db, "mem", self, event.getSource());
                trigger(r, networkPort);
                buffer = merge(_view_mem, _recieved_buffer.getDescriptors());
                synchronized (_view_change) {
                    _view_mem = selectView(rank(buffer, event.getType()));
                }

            }
        }

    };

    // Handles descriporbuffer responses. 
    // Depeding on the type merges the correct view with the respose view into a buffer. Selects the best peers from that buffer for the final view.
    Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
        @Override
        public void handle(ExchangeMsg.Response event) {

            DescriptorBuffer _recieved_buffer = event.getSelectedBuffer();
            ArrayList<PeerDescriptor> buffer;
            if (event.getType().equals("cpu")) {
                buffer = merge(_view_cpu, _recieved_buffer.getDescriptors());
                synchronized (_view_change) {
                    _view_cpu = selectView(rank(buffer, event.getType()));
                }
            } else {
                buffer = merge(_view_mem, _recieved_buffer.getDescriptors());
                synchronized (_view_change) {
                    _view_mem = selectView(rank(buffer, event.getType()));
                }
            }
        }
    };

    // Merges lists
    private ArrayList<PeerDescriptor> merge(ArrayList<PeerDescriptor> _merge_with, List<PeerDescriptor> _to_merge) {
        ArrayList<PeerDescriptor> _merged = new ArrayList<PeerDescriptor>(_merge_with);
        for (PeerDescriptor pd : _to_merge) {
            if (!_merged.contains(pd)) {
                _merged.add(pd);
            }
        }

        return _merged;

    }

    // Rankes list of peerdescriptor depending on their type cpu/memory
    private ArrayList<PeerDescriptor> rank(ArrayList<PeerDescriptor> _to_rank, String type) {

        ArrayList<PeerDescriptor> _ranked = new ArrayList<PeerDescriptor>(_to_rank);

        Collections.sort(_ranked, new ComparatorByUtility(type));

        return _ranked;

    }

    // Rankes entries the same way rank does and then selects a peer from that ranked list. Default selects best peer.
    public PeerDescriptor getSoftMaxPeerDescriptor(List<PeerDescriptor> entries, String type) {
        if (type.equals("cpu")) {
            Collections.sort(entries, new ComparatorByUtility(type));
        } else {
            Collections.sort(entries, new ComparatorByUtility(type));
        }
        double rnd = r.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        int j = entries.size() + 1;
        for (int i = 0; i < entries.size(); i++) {
            // get inverse of values - lowest have highest value.
            double val = j;
            j--;
            values[i] = Math.exp(val / tmanConfiguration.getTemperature());
            total += values[i];
        }

        for (int i = 0; i < values.length; i++) {
            if (i != 0) {
                values[i] += values[i - 1];
            }
            // normalise the probability for this entry
            double normalisedUtility = values[i] / total;
            if (normalisedUtility >= rnd) {
                return entries.get(i);
            }
        }
        return entries.get(entries.size() - 1);
    }

    //Make sure to keep view at constant size. If the vew is bigger than it should be, then remove worst peers.    
    private ArrayList<PeerDescriptor> selectView(ArrayList<PeerDescriptor> list) {

        while (list.size() > _view_size) {
            list.remove(list.size() - 1);
        }
        return list;
    }

}
