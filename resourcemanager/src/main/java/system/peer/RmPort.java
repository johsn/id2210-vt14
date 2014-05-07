package system.peer;

import common.simulation.BatchJob;
import common.simulation.RequestResource;
import se.sics.kompics.PortType;

public class RmPort extends PortType {{
	positive(RequestResource.class);
        positive(BatchJob.class);
}}
