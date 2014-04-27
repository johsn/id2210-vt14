package resourcemanager.system.peer.rm;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class CheckTimeout extends Timeout{
	
	private int id;
	protected CheckTimeout(ScheduleTimeout request, int id) {
		super(request);
		this.id = id;
	}
	
	public int getId(){
		return id;
	}

}