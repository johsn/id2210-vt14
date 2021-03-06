package common.simulation.scenarios;

import java.util.Random;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
        
	private static SimulationScenario scenario = new SimulationScenario() {{
                
                final Random rnd = new Random();
		StochasticProcess process0 = new StochasticProcess() {{
			eventInterArrivalTime(constant(1000));
			raise(100, Operations.peerJoin(), 
                                uniform(0, Integer.MAX_VALUE), 
                                constant(8), constant(16000)
                             );
		}};
                
		StochasticProcess t1 = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1000,Operations.requestResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(2), constant(0),
                                constant(1000*60*1) // 1 minute
                                );
		}};
                
                StochasticProcess t2 = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(300,Operations.requestBatch(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(3),
                                constant(2), constant(2000),
                                constant(1000*60*1) // 1 minute
                                );
		}};
                
                StochasticProcess t3 = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1000,Operations.requestResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(2), constant(0),
                                constant(1000*60*1) // 1 minute
                                );
		}};
                
                // TODO - not used yet
		StochasticProcess failPeersProcess = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.peerFail, 
                                uniform(0, Integer.MAX_VALUE));
		}};
                
		StochasticProcess terminateProcess = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.terminate);
		}};
                
                StochasticProcess killExperiment = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.kill);
		}};
		process0.start();
                t2.startAfterTerminationOf(2000, process0);
                //failPeersProcess.startAfterStartOf(180000, process0);
                terminateProcess.startAfterTerminationOf(100*1000, t2);
                killExperiment.startAfterTerminationOf(1000, terminateProcess);
	}};

	// -------------------------------------------------------------------
	public Scenario1() {
		super(scenario);
	}
}
