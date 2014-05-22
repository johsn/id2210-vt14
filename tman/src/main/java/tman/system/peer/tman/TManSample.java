package tman.system.peer.tman;

import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.ArrayList;


import se.sics.kompics.Event;


public class TManSample {
	

    public static class Cpu extends Event {

        private final ArrayList<PeerDescriptor> _partners;
        public Cpu(ArrayList<PeerDescriptor> partners) 
        {
            _partners = partners;
        }
        
        public ArrayList<PeerDescriptor> getSample()
        {
            return _partners;
        }
    }

    public static class Mem extends Event {
        
        private final ArrayList<PeerDescriptor> _partners;
        public Mem(ArrayList<PeerDescriptor> partners){
            
            _partners = partners;
        }
        
        public ArrayList<PeerDescriptor> getSample()
        {
            return _partners;
        }
    }
}
