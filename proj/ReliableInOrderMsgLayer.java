import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Layer above the basic messaging layer that provides reliable, in-order
 * delivery in the absence of faults. This layer does not provide much more than
 * the above.
 * 
 * At a minimum, the student should extend/modify this layer to provide
 * reliable, in-order message delivery, even in the presence of node failures.
 */
public class ReliableInOrderMsgLayer {
	public static int TIMEOUT = 3;
	
	private HashMap<Integer, InChannel> inConnections;
	private HashMap<Integer, OutChannel> outConnections;
	private RIONode n;
	private int nextSessionId = 0;

	/**
	 * Constructor.
	 * 
	 * @param destAddr
	 *            The address of the destination host
	 * @param msg
	 *            The message that was sent
	 * @param timeSent
	 *            The time that the ping was sent
	 */
	public ReliableInOrderMsgLayer(RIONode n) {
		inConnections = new HashMap<Integer, InChannel>();
		outConnections = new HashMap<Integer, OutChannel>();
		this.n = n;
	}
	
	/**
	 * Receive a data packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void receiveRPC(int from, byte[] msg) {
		RIOPacket riopkt = RIOPacket.unpack(msg);

		InChannel in = inConnections.get(from);
		if((in == null && riopkt.hasSessionId()) || (in != null && (riopkt.getSessionId() != in.getSessionId()))) {
			sendExpiredSessionError(from);
			return;
		}else if(in == null || !riopkt.hasSessionId()){
			System.out.println("Fatal Error: Node " + from + " doesn't have a sessionId on server " + this.n.addr + " but didn't request one.");
		}else{
			if(!riopkt.hasSessionId()){
				System.out.println("Fatal Error: Node " + from + " doesn't have a sessionId on server " + this.n.addr + " but didn't request one.");
			}else{
				n.send(from, Protocol.ACK, Utility.stringToByteArray("" + riopkt.getSeqNum()));
			}
		}
		
		LinkedList<RIOPacket> toBeDelivered = in.gotPacket(riopkt);
		
		for(RIOPacket p: toBeDelivered) {
			//System.out.println(p.getSeqNum() + " " + Protocol.protocolToString(p.getProtocol()));
			// deliver in-order the next sequence of packets
			n.onRIOReceive(from, p.getProtocol(), p.getPayload());
		}
	}
	
	public void receiveEstablishSession(int from){
		InChannel in = inConnections.get(from);
		if(in == null){
			in = new InChannel(nextSessionId);
			inConnections.put(from, in);
			nextSessionId++;
		}
		
		n.send(from, Protocol.ACK_SESSION, Utility.stringToByteArray(in.getSessionId() + " " + in.getLastSeqNumDelivered()));
	}
	
	private void sendExpiredSessionError(int from){
		this.inConnections.remove(from);
		InChannel in = new InChannel(nextSessionId);
		nextSessionId++;
		inConnections.put(from, in);
		n.send(from, Protocol.EXPIRED_SESSION, Utility.stringToByteArray(in.getSessionId() + ""));
	}
	
	public void receiveExpiredSessionError(Integer from, byte[] msg) {
		int newSessionId = Integer.parseInt(Utility.byteArrayToString(msg));
		
		this.outConnections.remove(from);
		this.outConnections.put(from, new OutChannel(this, this.n, from, newSessionId));
		
		System.out.println("Node " + this.n.addr + ": Error: Session expired on server " + from);
	}
	
	public void receiveSessionAck(int from, byte[] msg){
		String[] parts = Utility.byteArrayToString(msg).split(" ");
		int session = Integer.parseInt(parts[0]);
		int seqNum = Integer.parseInt(parts[1]);
		outConnections.get(from).gotSessionACK(session, seqNum);
	}
	
	/**
	 * Receive an acknowledgment packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void receiveAck(int from, byte[] msg) {
		int seqNum = Integer.parseInt( Utility.byteArrayToString(msg) );
		outConnections.get(from).gotACK(seqNum);
	}

	/**
	 * Send a packet using this reliable, in-order messaging layer. Note that
	 * this method does not include a reliable, in-order broadcast mechanism.
	 * 
	 * @param destAddr
	 *            The address of the destination for this packet
	 * @param protocol
	 *            The protocol identifier for the packet
	 * @param payload
	 *            The payload to be sent
	 */
	public void sendRIO(int destAddr, int protocol, byte[] payload) {
		OutChannel out = outConnections.get(destAddr);
		if(out == null) {
			out = new OutChannel(this, this.n, destAddr);
			outConnections.put(destAddr, out);
		}
		
		out.sendRIOPacket(protocol, payload);
	}

	/**
	 * Callback for timeouts while waiting for an ACK.
	 * 
	 * This method is here and not in OutChannel because OutChannel is not a
	 * public class.
	 * 
	 * @param destAddr
	 *            The receiving node of the unACKed packet
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(Integer destAddr, Integer seqNum) {
		outConnections.get(destAddr).onTimeout(n, seqNum);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(Integer i: inConnections.keySet()) {
			sb.append(inConnections.get(i).toString() + "\n");
		}
		
		return sb.toString();
	}
}

/**
 * Representation of an incoming channel to this node
 */
class InChannel {
	private int lastSeqNumDelivered;
	private HashMap<Integer, RIOPacket> outOfOrderMsgs;
	private int sessionId;
	
	InChannel(int nextSessionId){
		lastSeqNumDelivered = -1;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
		sessionId = nextSessionId;
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
	
	public int getLastSeqNumDelivered(){
		return this.lastSeqNumDelivered;
	}
	
	public int getSessionId() {
		return this.sessionId;
	}
	
	@Override
	public String toString() {
		return "last delivered: " + lastSeqNumDelivered + ", outstanding: " + outOfOrderMsgs.size();
	}
}

/**
 * Representation of an outgoing channel to this node
 */
class OutChannel {
	private HashMap<Integer, RIOPacket> unACKedPackets;
	private int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private RIONode n;
	
	private int destAddr;
	private int sessionId;
	
	private boolean establishingSession;
	private Queue<RIOPacket> queuedCommands;
	
	OutChannel(ReliableInOrderMsgLayer parent, RIONode n, int destAddr){
		this(parent, n, destAddr, -1);
	}
	
	OutChannel(ReliableInOrderMsgLayer parent, RIONode n, int destAddr, int sessionId){
		lastSeqNumSent = -1;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		
		this.parent = parent;
		this.n = n;
		this.sessionId = sessionId;
		this.destAddr = destAddr;
		
		establishingSession = false;
		queuedCommands = new LinkedList<RIOPacket>();
	}
	
	/**
	 * Send a new RIOPacket out on this channel.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param protocol
	 *            The protocol identifier of this packet
	 * @param payload
	 *            The payload to be sent
	 */
	protected void sendRIOPacket(int protocol, byte[] payload) {
		RIOPacket newPkt = new RIOPacket(protocol, sessionId, ++lastSeqNumSent, payload);
		
		if(establishingSession){
			this.queuedCommands.add(newPkt);
		}else if(this.sessionId == -1){
			this.queuedCommands.add(newPkt);
			this.establishSession();
		}else{
			sendRIOPacket(newPkt, true);
		}
	}
	
	public void establishSession(){
		RIOPacket sessionPkt = new RIOPacket(Protocol.ESTB_SESSION, ++lastSeqNumSent, Utility.stringToByteArray(""));
		this.sendRIOPacket(sessionPkt, false);
	}
	
	private void sendRIOPacket(RIOPacket pkt, boolean pack){
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			unACKedPackets.put(lastSeqNumSent, pkt);
			if(pack)
				n.send(destAddr, Protocol.DATA, pkt.pack());
			else
				n.send(destAddr, pkt.getProtocol(), pkt.getPayload());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, lastSeqNumSent }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Called when a timeout for this channel triggers
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(RIONode n, Integer seqNum) {
		if(unACKedPackets.containsKey(seqNum)) {
			resendRIOPacket(n, seqNum);
		}
	}
	
	/**
	 * Called when we get an ACK back. Removes the outstanding packet if it is
	 * still in unACKedPackets.
	 * 
	 * @param seqNum
	 *            The sequence number that was just ACKed
	 */
	protected void gotACK(int seqNum) {
		unACKedPackets.remove(seqNum);
	}
	
	protected void gotSessionACK(int sessionId, int seqNum) {
		if(this.sessionId == -1){
			this.establishingSession = false;
			this.unACKedPackets = new HashMap<Integer, RIOPacket>();
			this.lastSeqNumSent = seqNum;
			this.sessionId = sessionId;
			while(!this.queuedCommands.isEmpty()){
				RIOPacket p = this.queuedCommands.poll();
				this.sendRIOPacket(p.getProtocol(), p.getPayload());
			}
		}
	}
	
	/**
	 * Resend an unACKed packet.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	private void resendRIOPacket(RIONode n, int seqNum) {
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			RIOPacket riopkt = unACKedPackets.get(seqNum);
			
			if(riopkt.getProtocol() == Protocol.ESTB_SESSION)
				n.send(destAddr, Protocol.ESTB_SESSION, riopkt.getPayload());
			else
				n.send(destAddr, Protocol.DATA, riopkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, seqNum }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}

