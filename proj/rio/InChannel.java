package rio;

import java.util.HashMap;
import java.util.LinkedList;

import packets.RIOPacket;
import packets.SessionPacket;
import protocols.Protocol;

import nodes.RIONode;

/**
 * Representation of an incoming channel to this node.
 * Stores the client's session information on the server side
 */
public class InChannel {
	private RIONode n;
	private int sourceAddr;
	private int lastSeqNumDelivered;
	private HashMap<Integer, RIOPacket> outOfOrderMsgs;
	private int sessionId;
	
	public InChannel(RIONode n, int sourceAddr, int sessionID){
		this(n, sourceAddr, sessionID, -1);
	}
	
	public InChannel(RIONode n, int sourceAddr, int sessionID, int lastSeqNumDelivered){
		this.n = n;
		this.sourceAddr = sourceAddr;
		this.lastSeqNumDelivered = lastSeqNumDelivered;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
		sessionId = sessionID;
	}

	/**
	 * Method called whenever we receive a data packet.
	 * 
	 * @param pkt
	 *            The packet
	 * @return A list of the packets that we can now deliver due to the receipt
	 *         of this packet
	 */
	public LinkedList<RIOPacket> gotPacket(RIOPacket pkt) {
		LinkedList<RIOPacket> pktsToBeDelivered = new LinkedList<RIOPacket>();
		int seqNum = pkt.getSeqNum();
		
		if(seqNum == lastSeqNumDelivered + 1) {
			// We were waiting for this packet
			pktsToBeDelivered.add(pkt);
			++lastSeqNumDelivered;
			deliverSequence(pktsToBeDelivered);
		}else if(seqNum > lastSeqNumDelivered + 1){
			// We received a subsequent packet and should store it
			outOfOrderMsgs.put(seqNum, pkt);
		}
		// Duplicate packets are ignored
		
		return pktsToBeDelivered;
	}

	/**
	 * Helper method to grab all the packets we can now deliver.
	 * 
	 * @param pktsToBeDelivered
	 *            List to append to
	 */
	private void deliverSequence(LinkedList<RIOPacket> pktsToBeDelivered) {
		while(outOfOrderMsgs.containsKey(lastSeqNumDelivered + 1)) {
			++lastSeqNumDelivered;
			pktsToBeDelivered.add(outOfOrderMsgs.remove(lastSeqNumDelivered));
		}
	}
	
	public void restart() {
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
		lastSeqNumDelivered = -1;
	}
	
	public int getLastSeqNumDelivered(){
		return this.lastSeqNumDelivered;
	}
	
	public int getSessionId() {
		return this.sessionId;
	}
	
	public void returnSessionPacket(int protocol, byte[] payload){
		RIOPacket pkt = new RIOPacket(Protocol.SESSION, -1, new SessionPacket(protocol, payload).pack(), -1);
		this.n.send(this.sourceAddr, Protocol.SESSION, pkt.pack());
	}
	
	@Override
	public String toString() {
		return this.sourceAddr + " " + this.sessionId + " " + this.lastSeqNumDelivered + "\n";
	}
}