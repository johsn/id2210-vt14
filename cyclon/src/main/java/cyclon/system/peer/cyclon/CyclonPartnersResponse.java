package cyclon.system.peer.cyclon;

import java.util.ArrayList;


import se.sics.kompics.Event;


public class CyclonPartnersResponse extends Event {
	ArrayList<PeerDescriptor> partners = new ArrayList<PeerDescriptor>();


	public CyclonPartnersResponse(ArrayList<PeerDescriptor> partners) {
		this.partners = partners;
	}
        
	public CyclonPartnersResponse() {
	}


	public ArrayList<PeerDescriptor> getPartners() {
		return this.partners;
	}
}
