import java.util.*;

public class AcceptorLayer {

	private PaxosLayer paxosLayer;
	
	private HashMap<Integer, HashMap<Integer, String>> promises; //instance num -> largest proposal number -> value

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
