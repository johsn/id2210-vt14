package common.simulation;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public class SimulatorPort extends PortType {{
	positive(PeerJoin.class);
	positive(PeerFail.class);
        positive(TerminateExperiment.class);
	positive(RequestResource.class);
        positive(BatchRequest.class);
	positive(TerminateExperiment.class);
	negative(TerminateExperiment.class);
}}
