import edu.washington.cs.cse490h.lib.Node;

/**
 * Extension to the Node class that adds support for a reliable, in-order
 * messaging layer.
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 */
public abstract class RIONode extends Node {
	private ReliableInOrderMsgLayer RIOLayer;
	protected CacheCoherenceLayer CCLayer;
	
	public static int NUM_NODES = 10;
	
	public RIONode() {
		RIOLayer = new ReliableInOrderMsgLayer(this);
		CCLayer = new CacheCoherenceLayer(this);
	}
	
	public ReliableInOrderMsgLayer getRIOLayer(){
		return this.RIOLayer;
	}
	
	public CacheCoherenceLayer getCCLayer(){
		return this.CCLayer;
	}
	
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if(protocol == Protocol.ACK) {
			this.RIOLayer.receiveAck(from, msg);
		}else if(protocol == Protocol.SESSION){
			this.RIOLayer.receiveSession(from, SessionPacket.unpack(msg));
		}else if(protocol == Protocol.RPC){
			this.RIOLayer.receiveRIO(from, RIOPacket.unpack(msg));
		}
	}

	/**
	 * Send a message using the reliable, in-order delivery layer
	 * 
	 * @param destAddr
	 *            The address to send to
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param payload
	 *            The payload of the message
	 */
	public void sendRIO(int destAddr, int protocol, byte[] payload) {
		CCLayer.sendCC(destAddr, protocol, payload);
	}

	
	
	@Override
	public String toString() {
		return CCLayer.toString();
	}
}
