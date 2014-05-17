package common.simulation.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
	private static SimulationScenario scenario = new SimulationScenario() {{
                
		StochasticProcess process0 = new StochasticProcess() {{
			eventInterArrivalTime(constant(1000));
			raise(10, Operations.peerJoin(), 
                                uniform(0, Integer.MAX_VALUE), 
                                constant(2), constant(2000)
                             );
		}};
                
		StochasticProcess _regular_request = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(100,Operations.requestResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(2), constant(2000),
                                constant(1000*60*1) // 1 minute
                                );
		}};
                
                StochasticProcess _batch_request = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(10,Operations.requestBatch(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(3),
                                constant(2), constant(2000),
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
		process0.start();
		//_regular_request.startAfterTerminationOf(2000, process0);
                _batch_request.startAfterTerminationOf(2000, process0);
                failPeersProcess.startAfterStartOf(30000, process0);
                terminateProcess.startAfterTerminationOf(100*1000, _batch_request);
	}};

	// -------------------------------------------------------------------
	public Scenario1() {
		super(scenario);
	}
}
