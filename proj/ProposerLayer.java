
public class ProposerLayer {

	private PaxosLayer paxosLayer;

	public ProposerLayer(PaxosLayer paxosLayer) {
		this.paxosLayer = paxosLayer;
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}

	public void receivedPromise(int from, PaxosPacket pkt) {
		
	}
}
