
public class AcceptorLayer {

	private PaxosLayer paxosLayer;

	public AcceptorLayer(PaxosLayer paxosLayer) {
		this.paxosLayer = paxosLayer;
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}
	
	public void receivedPrepare(int from, PaxosPacket pkt){
		
	}
	
	public void receivedPropose(int from, PaxosPacket pkt){
		
	}
}
