import edu.washington.cs.cse490h.lib.Utility;


public class PaxosLayer {
	
	public static final int[] ACCEPTORS = {0,1,2,3,4};
	public static final int[] LEARNERS = {0,1,2,3,4};
	
	private TransactionLayer txnLayer;
	private ProposerLayer propLayer;
	private AcceptorLayer accLayer;
	private LearnerLayer learnLayer;
	public  DistNode n;
	private Election e;
	private boolean isServer;
	
	public PaxosLayer(TransactionLayer txn, boolean isServer){
		this.txnLayer = txn;
		this.isServer = isServer;
		if(isServer){
			this.n = this.txnLayer.n;
			this.propLayer = new ProposerLayer(this);
			this.accLayer = new AcceptorLayer(this);
			this.learnLayer = new LearnerLayer(this);
		}
	}
	
	public LearnerLayer getLearnerLayer(){
		return learnLayer;
	}
	
	public void onReceive(int from, byte[] payload) {
		PaxosPacket pkt = PaxosPacket.unpack(payload);
		switch(pkt.getProtocol()){
			case PaxosProtocol.ACCEPT:
			case PaxosProtocol.LEARN:
				this.learnLayer.receive(from, pkt);
			case PaxosProtocol.PREPARE:
				this.accLayer.receivedPrepare(from, pkt);
			case PaxosProtocol.PROMISE:
				this.propLayer.receivedPromise(from, pkt);
			case PaxosProtocol.PROPOSE:
				this.accLayer.receivedPropose(from, pkt);
			case PaxosProtocol.RECOVERY:
				this.accLayer.receivedRecovery(from, pkt);
			case PaxosProtocol.RECOVERY_ACCEPTED:
				this.propLayer.receivedRecovery(from, pkt);
			case PaxosProtocol.RECOVERY_CHOSEN:
				this.propLayer.receivedRecovery(from, pkt);
			case PaxosProtocol.REJECT:
				this.propLayer.receivedPromise(from, pkt);
			case PaxosProtocol.ELECT:
				this.receivedElect(from, pkt);
		}
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.txnLayer.send(dest, TXNProtocol.PAXOS, pkt.pack());
	}
	
	public int size(){
		return PaxosLayer.ACCEPTORS.length;
	}
	
	private void receivedElect(int from, PaxosPacket pkt){
		if(isServer){
			this.send(from, new PaxosPacket(PaxosProtocol.ELECT, -1, this.learnLayer.getLargestInstanceNum(), new byte[0]));
		}else if(this.e != null){
			this.e.propose(from, pkt.getInstanceNumber());
			if(this.e.hasMajority()){
				Pair<Integer, Integer> tmp = this.e.elect();
				this.txnLayer.elect(tmp.t1, tmp.t2);
				this.e = null;
			}
		}
	}

	public int electLeader() {
		this.e = new Election(this.size());
		for(int addr : ACCEPTORS)
			this.send(addr, new PaxosPacket(PaxosProtocol.ELECT, -1, -1, new byte[0]));
		return -1;
	}
}
