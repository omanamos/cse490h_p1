import java.util.HashMap;
import java.util.Map;


public class TransactionLayer {

	private final static int MASTER_NODE = 0;
	
	private RIONode n;
	private ReliableInOrderMsgLayer RIOLayer;
	private Map<String, File> cache;
	
	public TransactionLayer(RIONode n, ReliableInOrderMsgLayer RIOLayer){
		this.cache = new HashMap<String, File>();
		this.n = n;
		this.RIOLayer = RIOLayer;
	}

	public void send(int server, int protocol, byte[] payload) {
		TXNPacket pkt = new TXNPacket(protocol, payload);
		this.RIOLayer.sendRIO(server, Protocol.RPC, pkt.pack());
	}
	
	public void onRPCReceive(int from, byte[] payload) {
		TXNPacket packet = TXNPacket.unpack(payload);
		if(this.n.addr == MASTER_NODE)
			masterReceive(from, packet);
		else
			slaveReceive(packet);
	}
	
	public void onTimeout(int from, byte[] payload){
		
	}
	
	private void masterReceive(int from, TXNPacket pkt){
		
	}
	
	private void slaveReceive(TXNPacket pkt){
		switch(pkt.getProtocol()){
		case TXNProtocol.WF:
			break;
		case TXNProtocol.WD:
			break;
		case TXNProtocol.ABORT:
			this.abort(true);
			break;
		}
	}


	
	
	
	
	
	/*=====================================================
	 * Methods DistNode uses to talk to TXNLayer
	 *=====================================================*/
	public void get(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void create(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void put(String fileName, String content) {
		// TODO Auto-generated method stub
		
	}

	public void append(String fileName, String content) {
		// TODO Auto-generated method stub
		
	}

	public void delete(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void abort(boolean retry) {
		// TODO Auto-generated method stub
		
	}

	public void commit() {
		// TODO Auto-generated method stub
		
	}

	public void start() {
		// TODO Auto-generated method stub
		
	}

}
