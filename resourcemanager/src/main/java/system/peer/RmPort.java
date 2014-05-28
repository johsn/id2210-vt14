package system.peer;

import common.simulation.BatchRequest;
import common.simulation.PrintAvarage;
import common.simulation.RequestResource;
import se.sics.kompics.PortType;

public class RmPort extends PortType {{
        positive(PrintAvarage.class);
        negative(PrintAvarage.class);
	positive(RequestResource.class);
        negative(RequestResource.class);
        positive(BatchRequest.class);
        negative(BatchRequest.class);
}}
