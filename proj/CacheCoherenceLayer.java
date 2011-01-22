import java.util.HashMap;


public class CacheCoherenceLayer {
	
	private ReliableInOrderMsgLayer RIOLayer;
	private RIONode n;
	private HashMap<String, File> cache;
	
	public CacheCoherenceLayer(RIONode n) {
		this.n = n;
		this.RIOLayer = new ReliableInOrderMsgLayer( n, this );
		this.cache = new HashMap<String, File>();
	}
	
	public void sendCC( int server, int commandType, byte[] payload) {
		this.RIOLayer.sendRIO(server, commandType, payload);
	}

	public void receiveSession(Integer from, SessionPacket unpack) {
		this.RIOLayer.receiveSession(from, unpack);
	}

	public void receiveData(RTNPacket unpack) {
		this.RIOLayer.receiveData(unpack);
	}

	public void receiveRPC(Integer from, RPCPacket unpack) {
		this.RIOLayer.receiveRPC(from, unpack);
	}

	public void receiveAck(Integer from, byte[] msg) {
		this.RIOLayer.receiveAck(from, msg);
	}

	public void onRIOReceive(int from, int protocol, byte[] payload) {
		n.onCCReceive(from, protocol, payload);
	}
	

}
