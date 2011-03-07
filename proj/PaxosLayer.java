
public class PaxosLayer {
	
	public static final int[] ACCEPTORS = {0,1,2,3,4};
	public static final int[] LEARNERS = {0,1,2,3,4};
	
	private TransactionLayer txnLayer;
	private ProposerLayer propLayer;
	private AcceptorLayer accLayer;
	private LearnerLayer learnLayer;
	public DistNode n;
	private Election e;
	private boolean isServer;
	
	public PaxosLayer(TransactionLayer txn, DistNode n, boolean isServer){
		this.txnLayer = txn;
		this.isServer = isServer;
		this.n = n;
	}
	
	public String toString(){
		return this.e == null ? "No Election" : this.e.toString();
	}
	
	public void start(){
		if(this.isServer){
			this.propLayer = new ProposerLayer(this, n);
			this.accLayer = new AcceptorLayer(this, n);
			this.learnLayer = new LearnerLayer(this, n);

			//The order here matters
			this.learnLayer.start();
			this.accLayer.start();
			this.propLayer.start();
		}
	}
	
	public LearnerLayer getLearnerLayer(){
		return learnLayer;
	}
	
	public TransactionLayer getTransactionLayer() {
		return this.txnLayer;
	}
	
	public ProposerLayer getProposerLayer() {
		return this.propLayer;
	}
	
	public AcceptorLayer getAcceptorLayer(){
		return this.accLayer;
	}
	
	public void onReceive(int from, int seqNum, byte[] payload) {
		PaxosPacket pkt = PaxosPacket.unpack(payload);
		switch(pkt.getProtocol()){
			case PaxosProtocol.ACCEPT:
			case PaxosProtocol.LEARN:
				this.learnLayer.receive(from, pkt);
				break;
			case PaxosProtocol.PREPARE:
				this.accLayer.receivedPrepare(from, pkt);
				break;
			case PaxosProtocol.PROMISE:
				this.propLayer.receivedPromise(from, pkt);
				break;
			case PaxosProtocol.PROPOSE:
				this.accLayer.receivedPropose(from, pkt);
				break;
			case PaxosProtocol.RECOVERY:
				this.accLayer.receivedRecovery(from, pkt);
				break;
			case PaxosProtocol.RECOVERY_ACCEPTED:
				this.propLayer.receivedRecovery(from, pkt);
				break;
			case PaxosProtocol.RECOVERY_CHOSEN:
				this.propLayer.receivedRecovery(from, pkt);
				break;
			case PaxosProtocol.REJECT:
				this.propLayer.receivedPromise(from, pkt);
				break;
			case PaxosProtocol.ELECT:
				this.receivedElect(from, seqNum, pkt);
				break;
		}
	}
	
	public void onTimeout(int dest, byte[] payload){
		PaxosPacket pkt = PaxosPacket.unpack(payload);
		if(pkt.getProtocol() == PaxosProtocol.ELECT && this.e != null){
			if(this.e.onTimeout()){
				this.e = null;
				this.n.printError("Node " + this.n.addr + ": Error: Election failed, a majority of requests timed out.");
				this.txnLayer.abort(false, false);
			}
		}
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.txnLayer.send(dest, TXNProtocol.PAXOS, pkt.pack());
	}
	
	public void rtn(int dest, int seqNum, PaxosPacket pkt){
		this.txnLayer.rtn(dest, TXNProtocol.PAXOS, seqNum, pkt.pack());
	}
	
	public int size(){
		return PaxosLayer.ACCEPTORS.length;
	}
	
	public void commit(Transaction txn){
		this.propLayer.receivedCommit(txn.toString());
	}
	
	public boolean hasElection(){
		return this.e != null;
	}
	
	public int electLeader() {
		this.e = new Election(this.size());
		for(int addr : ACCEPTORS)
			this.send(addr, new PaxosPacket(PaxosProtocol.ELECT, -1, -1, new byte[0]));
		return -1;
	}
	
	private void receivedElect(int from, int seqNum, PaxosPacket pkt){
		if(isServer){
			this.rtn(from, seqNum, new PaxosPacket(PaxosProtocol.ELECT, -1, Math.max(this.learnLayer.getLargestInstanceNum(), this.accLayer.getMaxInstanceNumber()), new byte[0]));
		}else if(this.e != null){
			this.e.propose(from, pkt.getInstanceNumber());
			if(this.e.hasMajority()){
				Pair<Integer, Integer> tmp = this.e.elect();
				this.txnLayer.elect(tmp.t1, tmp.t2);
				this.e = null;
			}
		}
	}
}
