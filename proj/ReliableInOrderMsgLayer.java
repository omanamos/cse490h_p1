import java.lang.reflect.Method;
import java.util.Collection;
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
	 * SERVER METHOD<br>
	 * Receive a data packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void receiveRPC(int from, RPCPacket pkt) {

		InChannel in = inConnections.get(from);
		if(in == null) { //Expired Session -> Server has crashed recently
			sendExpiredSessionError(from);
			return;
		}else{ //Normal RPC
			in.returnAck(Utility.stringToByteArray("" + pkt.getSeqNum()));
		}
		
		LinkedList<RPCPacket> toBeDelivered = in.gotPacket(pkt);
		
		for(RIOPacket p: toBeDelivered) {
			//System.out.println(p.getSeqNum() + " " + Protocol.protocolToString(p.getProtocol()));
			// deliver in-order the next sequence of packets
			n.onRIOReceive(from, p.getProtocol(), p.getPayload());
		}
	}
	
	public void receiveSession(int from, SessionPacket pkt){
		switch(pkt.getProtocol()){
			case SessionProtocol.ACK_SESSION:
				this.receiveAckSession(from, pkt.getPayload());
				break;
			case SessionProtocol.ESTB_SESSION:
				this.receiveEstablishSession(from);
				break;
			case SessionProtocol.EXPIRED_SESSION:
				this.receiveExpiredSessionError(from, pkt.getPayload());
				break;
		}
	}
	
	/**
	 * <pre>
	 * SERVER METHOD
	 * Called when the server receives an establish session packet
	 * Sends an ACK_SESSION packet back to the client:
	 *      payload -> sessionID curSeqNum
	 * </pre> 
	 * @param from client node the packet came from
	 */
	private void receiveEstablishSession(int from){
		InChannel in = inConnections.get(from);
		if(in == null){
			in = new InChannel(n, from, nextSessionId);
			inConnections.put(from, in);
			nextSessionId++;
		}
		
		in.returnSessionPacket(SessionProtocol.ACK_SESSION, Utility.stringToByteArray(in.getSessionId() + " " + in.getLastSeqNumDelivered()));
	}
	
	/**
	 * SERVER METHOD<br>
	 * Called when the server receives a packet with an expired/invalid sessionId
	 * Creates a new session on the InChannel and sends back the new sessionId
	 * in a EXPIRED_SESSION packet
	 * @param from client node packet came from
	 */
	private void sendExpiredSessionError(int from){
		this.inConnections.remove(from);
		InChannel in = new InChannel(n, from, nextSessionId);
		nextSessionId++;
		inConnections.put(from, in);
		in.returnSessionPacket(SessionProtocol.EXPIRED_SESSION, Utility.stringToByteArray(in.getSessionId() + ""));
	}
	
	/**
	 * SERVER METHOD<br>
	 * @param destAddr source of request
	 * @param protocol type of return
	 * @param payload data to send
	 */
	public void returnRIO(int destAddr, int protocol, byte[] payload){
		InChannel in = inConnections.get(destAddr);
		if(in == null){
			this.sendExpiredSessionError(destAddr);
		}
		
		in.returnRIOPacket(protocol, payload);
	}
	
	
	
	
	
	
	
	
	/**
	 * CLIENT METHOD<br>
	 * Prints out the message returned.
	 * @param msg payload to print out
	 */
	public void receiveData(RTNPacket pkt){
		System.out.println(Utility.byteArrayToString(pkt.getPayload()));
	}
	
	/**
	 * CLIENT METHOD<br>
	 * Called when the client receives an EXPIRED_SESSION packet.
	 * Resets the OutChannel going to the server that sent the EXPIRED_SESSION packet.
	 * @param from server node packet came from
	 * @param msg new SessionID from the server
	 */
	private void receiveExpiredSessionError(Integer from, byte[] msg) {
		int newSessionId = Integer.parseInt(Utility.byteArrayToString(msg));
		
		this.outConnections.remove(from);
		this.outConnections.put(from, new OutChannel(this, this.n, from, newSessionId));
		
		System.out.println("Node " + this.n.addr + ": Error: Session expired on server " + from);
	}
	
	/**
	 * CLIENT METHOD<br>
	 * Called when the client receives an ACK_SESSION packet.
	 * Tells the OutChannel that it has established a session with the server.
	 * @param from
	 * @param msg
	 */
	private void receiveAckSession(int from, byte[] msg){
		String[] parts = Utility.byteArrayToString(msg).split(" ");
		int session = Integer.parseInt(parts[0]);
		int seqNum = Integer.parseInt(parts[1]);
		outConnections.get(from).receiveAckSession(session, seqNum);
	}
	
	/**
	 * CLIENT METHOD<br>
	 * Receive an acknowledgment packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void receiveAck(int from, byte[] msg) {
		int seqNum = Integer.parseInt( Utility.byteArrayToString(msg) );
		outConnections.get(from).receiveAck(seqNum);
	}

	/**
	 * CLIENT METHOD<br>
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
		
		out.sendRPCPacket(protocol, payload);
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
 * Representation of an incoming channel to this node.
 * Stores the client's session information on the server side
 */
class InChannel {
	private RIONode n;
	private int sourceAddr;
	private int lastSeqNumDelivered;
	private HashMap<Integer, RPCPacket> outOfOrderMsgs;
	private int sessionId;
	
	InChannel(RIONode n, int sourceAddr, int nextSessionId){
		this.n = n;
		this.sourceAddr = sourceAddr;
		lastSeqNumDelivered = -1;
		outOfOrderMsgs = new HashMap<Integer, RPCPacket>();
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
	public LinkedList<RPCPacket> gotPacket(RPCPacket pkt) {
		LinkedList<RPCPacket> pktsToBeDelivered = new LinkedList<RPCPacket>();
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
	private void deliverSequence(LinkedList<RPCPacket> pktsToBeDelivered) {
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
	
	public void returnRIOPacket(int protocol, byte[] payload){
		this.n.send(this.sourceAddr, Protocol.RTN, new RTNPacket(protocol, payload).pack());
	}
	
	public void returnAck(byte[] payload){
		this.n.send(this.sourceAddr, Protocol.ACK, payload);
	}
	
	public void returnSessionPacket(int protocol, byte[] payload){
		this.n.send(this.sourceAddr, Protocol.SESSION, new SessionPacket(protocol, payload).pack());
	}
	
	@Override
	public String toString() {
		return "last delivered: " + lastSeqNumDelivered + ", outstanding: " + outOfOrderMsgs.size();
	}
}

/**
 * Representation of an outgoing channel to this node.
 * Stores the clients session information on the client side.
 */
class OutChannel {
	private HashMap<Integer, RIOPacket> unACKedPackets;
	private int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private RIONode n;
	
	private int destAddr;
	private int sessionID;
	
	private boolean establishingSession; //true if this connection is currently setting up the session.
	private Queue<RIOPacket> queuedCommands; //fills up with queued commands while the session is being established.
	
	OutChannel(ReliableInOrderMsgLayer parent, RIONode n, int destAddr){
		this(parent, n, destAddr, -1);
	}
	
	OutChannel(ReliableInOrderMsgLayer parent, RIONode n, int destAddr, int sessionId){
		lastSeqNumSent = -1;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		
		this.parent = parent;
		this.n = n;
		this.sessionID = sessionId;
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
	protected void sendRPCPacket(int protocol, byte[] payload) {
		RPCPacket pkt = new RPCPacket(protocol, lastSeqNumSent + 1, payload, sessionID);
		
		if(establishingSession){			//Connection establishing a session
			this.queuedCommands.add(pkt);
		}else if(this.sessionID == -1){		//Connection needs to establish a session
			this.queuedCommands.add(pkt);
			this.establishSession();
		}else{								//Session is already set up. Proceed normally.
			byte[] packed = pkt.pack();
			if(packed.length > Protocol.MAX_PROTOCOL){
				System.out.println(DistNode.buildErrorString(this.destAddr, this.n.addr, protocol, "", Error.ERR_30));
			}else{
				lastSeqNumSent++;
				this.createTimeoutListener(pkt);
				n.send(destAddr, Protocol.RPC, pkt.pack());
			}
		}
	}
	
	/**
	 * Sends the server an ESTB_SESSION packet.<br>
	 * Sets the state of this OutChannel to cache commands while
	 * the session is being established.
	 */
	public void establishSession(){
		SessionPacket pkt = new SessionPacket(SessionProtocol.ESTB_SESSION, new byte[0]);
		this.establishingSession = true;
		this.createTimeoutListener(pkt);
		n.send(destAddr, Protocol.SESSION, pkt.pack());
	}
	
	private void createTimeoutListener(RIOPacket pkt){
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			unACKedPackets.put(lastSeqNumSent, pkt);
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
	protected void receiveAck(int seqNum) {
		unACKedPackets.remove(seqNum);
	}
	
	/**
	 * Called to set up session on client side, after server has responded with sessionID and current seqNum for given sessionID.
	 * Also sets state of this connection to be not setting up session and sends commands that were queued while the session was being established.
	 * Does nothing if the session is already set.
	 * @param sessionId set sessionId to for this connection
	 * @param seqNum set lastSeqNumSent to for this connection (could be not initial value in case of client failure)
	 */
	protected void receiveAckSession(int sessionId, int seqNum) {
		if(this.sessionID == -1){
			System.out.println(1);
			this.establishingSession = false;
			this.unACKedPackets = new HashMap<Integer, RIOPacket>();
			this.lastSeqNumSent = seqNum;
			this.sessionID = sessionId;
			
			while(!this.queuedCommands.isEmpty()){
				RIOPacket p = this.queuedCommands.poll();
				this.sendRPCPacket(p.getProtocol(), p.getPayload());
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
			RIOPacket pkt = unACKedPackets.get(seqNum);
			System.out.println("Values: " + toS(this.unACKedPackets.values()) + " Keys: " + toS(this.unACKedPackets.keySet()));
			System.out.println("SeqNum: " + seqNum + " Protocol: " + pkt.getProtocol());
			if(pkt instanceof SessionPacket)
				n.send(destAddr, Protocol.SESSION, ((SessionPacket)pkt).pack());
			else
				n.send(destAddr, Protocol.RPC, ((RPCPacket)pkt).pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, seqNum }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private <E> String toS(Collection<E> c){
		String rtn = "";
		for(E e : c){
			rtn += e.toString() + ", ";
		}
		return rtn;
	}
}

