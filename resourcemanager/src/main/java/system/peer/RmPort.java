package system.peer;

import common.simulation.BatchRequest;
import common.simulation.RequestResource;
import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public class RmPort extends PortType {{
        positive(TerminateExperiment.class);
        negative(TerminateExperiment.class);
	positive(RequestResource.class);
        negative(RequestResource.class);
        positive(BatchRequest.class);
        negative(BatchRequest.class);
}}
