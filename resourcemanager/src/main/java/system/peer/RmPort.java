package system.peer;

import common.simulation.BatchRequest;
import common.simulation.PrintAverage;
import common.simulation.RequestResource;
import se.sics.kompics.PortType;

public class RmPort extends PortType {{
        positive(PrintAverage.class);
        negative(PrintAverage.class);
	positive(RequestResource.class);
        negative(RequestResource.class);
        positive(BatchRequest.class);
        negative(BatchRequest.class);
}}
