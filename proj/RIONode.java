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
	//protected ReliableInOrderMsgLayer RIOLayer;
	protected CacheCoherenceLayer CCLayer;
	
	public static int NUM_NODES = 10;
	
	public RIONode() {
		//RIOLayer = new ReliableInOrderMsgLayer(this);
		CCLayer = new CacheCoherenceLayer(this);
	}
	
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if(protocol == Protocol.ACK) {
			CCLayer.receiveAck(from, ACKPacket.unpack(msg));
		}else if(protocol == Protocol.SESSION){
			CCLayer.receiveSession(from, SessionPacket.unpack(msg));
		}else if(protocol == Protocol.RPC){
			CCLayer.receiveRPC(from, RPCPacket.unpack(msg));
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

	/**
	 * Method that is called by the CC layer when a message is to be delivered.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param msg
	 *            The message that was received
	 */
	public abstract void onCCReceive(Integer from, int protocol, byte[] msg);
	
	
	
	
	@Override
	public String toString() {
		return CCLayer.toString();
	}
}
