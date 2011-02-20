
public class PaxosLayer {
	
	public static final int[] ACCEPTORS = {0,1,2,3,4};
	public static final int[] LEARNERS = {0,1,2,3,4};
	
	private TransactionLayer txnLayer;
	private ProposerLayer propLayer;
	private AcceptorLayer accLayer;
	private LearnerLayer learnLayer;
	
	public PaxosLayer(TransactionLayer txn){
		this.txnLayer = txn;
		this.propLayer = new ProposerLayer(this);
		this.accLayer = new AcceptorLayer(this);
		this.learnLayer = new LearnerLayer(this);
		
	}
	
	public void onReceive(int from, byte[] payload) {
		PaxosPacket pkt = PaxosPacket.unpack(payload);
		switch(pkt.getProtocol()){
			case PaxosProtocol.ACCEPT:
				this.learnLayer.receivedAccept(from, pkt);
			case PaxosProtocol.LEARN:
				this.learnLayer.receivedLearn(from, pkt);
			case PaxosProtocol.PREPARE:
				this.accLayer.receivedPrepare(from, pkt);
			case PaxosProtocol.PROMISE:
				this.propLayer.receivedPromise(from, pkt);
			case PaxosProtocol.PROPOSE:
				this.accLayer.receivedPropose(from, pkt);
		}
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.txnLayer.send(dest, TXNProtocol.PAXOS, pkt.pack());
	}
}
