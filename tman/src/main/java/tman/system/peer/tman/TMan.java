package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import java.util.ArrayList;

import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.DescriptorBuffer;
import cyclon.system.peer.cyclon.PeerDescriptor;
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

import tman.simulator.snapshot.Snapshot;

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
    
    private TManConfiguration tmanConfiguration;
    private Random r;
    private AvailableResources availableResources; 
    PeerDescriptor _mydescriptor;
    private UUID _id;
    private int _view_size = 5;

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
            
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(period,period);
            spt.setTimeoutEvent(new TManPost(spt));
            trigger(spt,timerPort);

        }
    };
    
    Handler<TManPost> handleTManPost = new Handler<TManPost>() {
        @Override
        public void handle(TManPost event) {
            
            TManSample.Cpu _sample_cpu = new TManSample.Cpu(_view_cpu);
            TManSample.Mem _sample_mem = new TManSample.Mem(_view_mem);
            
            trigger(_sample_cpu,tmanPort);
            trigger(_sample_mem,tmanPort);
            
        }
    };

    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {
            
            ArrayList<PeerDescriptor> _buffer_cpu = new ArrayList<PeerDescriptor>();
            ArrayList<PeerDescriptor> _buffer_mem = new ArrayList<PeerDescriptor>();
            
            _view_cpu = rank(_view_cpu,"cpu");
            _view_mem = rank(_view_mem,"mem");
            
            PeerDescriptor peer_cpu = selectPeer(_view_cpu);
            PeerDescriptor peer_mem = selectPeer(_view_mem);
            
            ArrayList<PeerDescriptor> _mydescriptorlist = new ArrayList<PeerDescriptor>();
            _mydescriptorlist.add(_mydescriptor);
            
            _buffer_cpu = merge(_view_cpu,_mydescriptorlist);
            _buffer_mem = merge(_view_mem,_mydescriptorlist);
            
            DescriptorBuffer db_cpu = new DescriptorBuffer(_mydescriptor,_buffer_cpu);
            DescriptorBuffer db_mem = new DescriptorBuffer(_mydescriptor,_buffer_mem);
            
            ExchangeMsg.Request r_cpu = new ExchangeMsg.Request(_id, db_cpu,"cpu", self, peer_cpu.getAddress());
            ExchangeMsg.Request r_mem = new ExchangeMsg.Request(_id, db_mem,"mem", self, peer_mem.getAddress());
            
            trigger(r_cpu,networkPort);
            trigger(r_mem,networkPort);
            
        }
    };

    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            List<PeerDescriptor> cyclonPartners = event.getSample();
            _view_cpu = merge(_view_cpu,cyclonPartners);
            _view_mem = merge(_view_mem,cyclonPartners);
        }
        
    };

    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            
            DescriptorBuffer _recieved_buffer = event.getRandomBuffer();
            ArrayList<PeerDescriptor> buffer;
            if(event.getType().equals("cpu"))
            {
                ArrayList<PeerDescriptor> _mydescriptorlist = new ArrayList<PeerDescriptor>();
                _mydescriptorlist.add(_mydescriptor);
                buffer = merge(_view_cpu,_mydescriptorlist);
                DescriptorBuffer db = new DescriptorBuffer(_mydescriptor,buffer);
                ExchangeMsg.Response r = new ExchangeMsg.Response(_id, db,"cpu", self, event.getSource());
                trigger(r,networkPort);
                buffer = merge(_view_cpu,_recieved_buffer.getDescriptors());
                _view_cpu = selectView(rank(buffer,event.getType()));
            }
            else
            {
                ArrayList<PeerDescriptor> _mydescriptorlist = new ArrayList<PeerDescriptor>();
                _mydescriptorlist.add(_mydescriptor);
                buffer = merge(_view_mem,_mydescriptorlist);
                DescriptorBuffer db = new DescriptorBuffer(_mydescriptor,buffer);
                ExchangeMsg.Response r = new ExchangeMsg.Response(_id, db,"mem", self, event.getSource());
                trigger(r,networkPort);
                buffer = merge(_view_mem,_recieved_buffer.getDescriptors());
                _view_mem = selectView(rank(buffer,event.getType()));
                
            }
        }
    };

    Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
        @Override
        public void handle(ExchangeMsg.Response event) {

            DescriptorBuffer _recieved_buffer = event.getSelectedBuffer();
            ArrayList<PeerDescriptor> buffer;
            if(event.getType().equals("cpu"))
            {
                buffer = merge(_view_cpu,_recieved_buffer.getDescriptors());
                _view_cpu = selectView(rank(buffer,event.getType()));
            }
            else
            {
                buffer = merge(_view_mem,_recieved_buffer.getDescriptors());
                _view_mem = selectView(rank(buffer,event.getType()));
            }
        }
    };
    
    private ArrayList<PeerDescriptor> merge(ArrayList<PeerDescriptor> _merge_with , List<PeerDescriptor> _to_merge)
    {
        ArrayList<PeerDescriptor> _merged = new ArrayList<PeerDescriptor>(_merge_with);
        for(PeerDescriptor pd : _to_merge)
        {
            if(!_merged.contains(pd))
            {
                _merged.add(pd);
            }
        }
            
        return _merged;
        
    }
    
     private ArrayList<PeerDescriptor> rank(ArrayList<PeerDescriptor> _to_rank,String type)
     {
         int index = 0;
         ArrayList<PeerDescriptor> _ranked = new ArrayList<PeerDescriptor>();
         if(type.equals("cpu"))
         {
             while(!_to_rank.isEmpty())
             {
                 PeerDescriptor _most_cpu_peer = takeLargestCpuPeer(_to_rank);
                 _to_rank.remove(_most_cpu_peer);
                 _ranked.add(index, _most_cpu_peer);
                 index++;
             }
         }
         else
         {
             while(!_to_rank.isEmpty())
             {
                 PeerDescriptor _most_mem_peer = takeLargestMemPeer(_to_rank);
                 _to_rank.remove(_most_mem_peer);
                 _ranked.add(index, _most_mem_peer);
                 index++;
             }
         }
        
         
         return _ranked;
        
     }
     
     private PeerDescriptor takeLargestCpuPeer(ArrayList<PeerDescriptor> _to_rank)
     {
         PeerDescriptor _current_peer = _to_rank.get(0);
         for(PeerDescriptor pd : _to_rank)
         {
             if(pd.getAvailableResources().getNumFreeCpus() > _current_peer.getAvailableResources().getNumFreeCpus())
             {
                 _current_peer = pd;
             }
         }
         
         return _current_peer;
     }
     
     

    private PeerDescriptor takeLargestMemPeer(ArrayList<PeerDescriptor> _to_rank) {
        
         PeerDescriptor _current_peer = _to_rank.get(0);
         for(PeerDescriptor pd : _to_rank)
         {
             if(pd.getAvailableResources().getFreeMemInMbs() > _current_peer.getAvailableResources().getFreeMemInMbs())
             {
                 _current_peer = pd;
             }
         }
         
         return _current_peer;
    }
    
    private PeerDescriptor selectPeer(ArrayList<PeerDescriptor> list)
    {
        return list.get(0);
    }
    
    

        private ArrayList<PeerDescriptor> selectView(ArrayList<PeerDescriptor> list) {
            
            if(list.size() > _view_size)
            {
                
                for(int index = _view_size;index < list.size();index++)
                {
                    list.remove(list.get(index));
                }
            }
            return list;
        }

        
    

       
    

}
