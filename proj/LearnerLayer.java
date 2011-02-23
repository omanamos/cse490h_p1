
public class LearnerLayer {

	private PaxosLayer paxosLayer;

	public LearnerLayer(PaxosLayer paxosLayer) {
		this.paxosLayer = paxosLayer;
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}
	
	public void receivedLearn(int from, PaxosPacket pkt){
		
	}
	
	public void receivedAccept(int from, PaxosPacket pkt){
		
	}
}
