package tman.system.peer.tman;

import se.sics.kompics.PortType;

public final class TManSamplePort extends PortType {{
	positive(TManSample.Cpu.class);
        positive(TManSample.Mem.class);
}}
