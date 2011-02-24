import java.util.LinkedList;
import java.util.Queue;


public class ProposerLayer {

	private PaxosLayer paxosLayer;
	private int promises;
	private int majority;
	private Queue<Commit> commits = new LinkedList<Commit>();
	private int proposalNumber;
	private int instanceNumber;
	

	public ProposerLayer(PaxosLayer paxosLayer) {
		this.paxosLayer = paxosLayer;
		this.promises = 0;
		this.majority = paxosLayer.ACCEPTORS.length/2 + 1;
		this.proposalNumber = 0;
		//TODO: fill gaps!!!, read from disk!!!
		this.instanceNumber = 0;
		
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}

	public void receivedPromise(int from, PaxosPacket pkt) {
		//log promise?
		promises++;
		if(promises >= majority){
			PaxosPacket proposal = createProposal();
			for(int acceptor : PaxosLayer.ACCEPTORS)
				send(acceptor, proposal);
		}
			
	}
	
	private PaxosPacket createProposal() {
		// TODO Auto-generated method stub
		return new PaxosPacket(PaxosProtocol.PROPOSE, this.proposalNumber, null, this.instanceNumber);
	}

	public void recievedCommit(int from, Commit commit){
		commits.add(commit);
	}
	
	
}
