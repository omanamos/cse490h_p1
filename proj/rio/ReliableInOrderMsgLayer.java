package rio;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import packets.RIOPacket;
import packets.SessionPacket;
import protocols.Protocol;
import protocols.SessionProtocol;

import nodes.DistNode;
import nodes.RIONode;

import transactions.TransactionLayer;

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
	public final static int TIMEOUT = 3;
	public final static int MAX_RETRY = 4;
	
	private Map<Integer, InChannel> inConnections;
	private Map<Integer, OutChannel> outConnections;
	private DistNode n;
	TransactionLayer TXNLayer;
	private int nextSessionId;

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
		this.n = (DistNode)n;
		this.nextSessionId = 0;
	}
	
	public void addConnections(Map<Integer, InChannel> connections, int maxSessionID){
		this.nextSessionId = maxSessionID + 1;
		this.inConnections.putAll(connections);
	}
	
	public void setTXNLayer(TransactionLayer TXNLayer){
		this.TXNLayer = TXNLayer;
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
	public void receiveRIO(int from, RIOPacket pkt) {
		InChannel in = inConnections.get(from);
		if(in == null) { //Expired Session -> Server has crashed recently
			sendExpiredSessionError(from);
			return;
		}else if(pkt.getSessionID() != in.getSessionId()){
			return;
		}
		
		this.sendAck(from, pkt.getSeqNum());
		
		LinkedList<RIOPacket> toBeDelivered = in.gotPacket(pkt);
		
		String content = "";
		for(Integer addr : this.inConnections.keySet()){
			content += this.inConnections.get(addr);
		}
		try {
			this.n.write(".sessions", content, false, true);
		} catch (IOException e) {
			this.n.printError("Node " + this.n.addr + ": Fatal Error: Could not update .sessions file");
		}
		
		for(RIOPacket p: toBeDelivered) {
			// deliver in-order the next sequence of packets
			TXNLayer.onReceive(from, p.getPayload());
		}
	}
	
	public void receiveSession(int from, RIOPacket wrapper){
		SessionPacket pkt = SessionPacket.unpack(wrapper.getPayload());
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
	 * CLIENT METHOD<br>
	 * Called when the client receives an ACK_SESSION packet.
	 * Tells the OutChannel that it has established a session with the server.
	 * @param from
	 * @param msg
	 */
	private void receiveAckSession(int from, byte[] msg){
		if( outConnections.get(from ) != null ) { 
			String[] parts = Utility.byteArrayToString(msg).split(" ");
			int session = Integer.parseInt(parts[0]);
			int seqNum = Integer.parseInt(parts[1]);
			outConnections.get(from).receiveAckSession(session, seqNum);
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
		InChannel in = inConnections.remove(from);
		in = new InChannel(n, from, nextSessionId);
		inConnections.put(from, in);
		nextSessionId++;
		
		this.updateSessions();
		in.returnSessionPacket(SessionProtocol.ACK_SESSION, Utility.stringToByteArray(in.getSessionId() + " " + in.getLastSeqNumDelivered()));
	}
	
	private void updateSessions(){
		try {
			String contents = "";
			for(InChannel in : this.inConnections.values())
				contents += in;
			this.n.write(".sessions", contents, false, true);
		} catch (IOException e) {
			this.n.printError("Node " + this.n.addr + ": Fatal Error: could not write session to disk");
		}
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
	
	private void sendAck(int from, int seqNum){
		this.n.send(from, Protocol.ACK, Utility.stringToByteArray(seqNum + ""));
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
		if( outConnections.get(from ) != null ) { 
			int seqNum = Integer.parseInt(Utility.byteArrayToString(msg));
			outConnections.get(from).receiveAck(seqNum);
		}
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
		
		out.sendRIOPacket(protocol, payload);
	}
	
	public void setHB(int destAddr, boolean heartbeat){
		OutChannel out = outConnections.get(destAddr);
		if(out == null) {
			out = new OutChannel(this, this.n, destAddr);
			outConnections.put(destAddr, out);
		}
		
		if(heartbeat)
			out.startHB();
		else
			out.stopHB();
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
