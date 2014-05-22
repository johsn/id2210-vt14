package cyclon.system.peer.cyclon;

import java.util.ArrayList;


import se.sics.kompics.Event;


public class CyclonSample extends Event {
	ArrayList<PeerDescriptor> nodes = new ArrayList<PeerDescriptor>();


	public CyclonSample(ArrayList<PeerDescriptor> nodes) {
		this.nodes = nodes;
	}
        
	public CyclonSample() {
	}


	public ArrayList<PeerDescriptor> getSample() {
		return this.nodes;
	}
}
