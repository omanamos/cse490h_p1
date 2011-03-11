package rio;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import packets.RIOPacket;
import packets.SessionPacket;
import packets.TXNPacket;
import protocols.Protocol;
import protocols.SessionProtocol;
import protocols.TXNProtocol;

import nodes.DistNode;
import nodes.RIONode;
import utils.Error;
import edu.washington.cs.cse490h.lib.Callback;

/**
 * Representation of an outgoing channel to this node.
 * Stores the clients session information on the client side.
 */
public class OutChannel {
	private HashMap<Integer, RIOPacket> unACKedPackets;
	private HashMap<Integer, Integer> pktRetries;
	
	private int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private DistNode n;
	
	private int destAddr;
	private int sessionID;
	
	private boolean establishingSession; //true if this connection is currently setting up the session.
	private Queue<RIOPacket> queuedCommands; //fills up with queued commands while the session is being established.
	
	private boolean heartbeat;
	
	public OutChannel(ReliableInOrderMsgLayer parent, DistNode n, int destAddr){
		this(parent, n, destAddr, -1);
	}
	
	public OutChannel(ReliableInOrderMsgLayer parent, DistNode n, int destAddr, int sessionId){
		lastSeqNumSent = -1;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		pktRetries = new HashMap<Integer, Integer>();
		
		this.parent = parent;
		this.n = n;
		this.sessionID = sessionId;
		this.destAddr = destAddr;
		
		establishingSession = false;
		queuedCommands = new LinkedList<RIOPacket>();
		
		this.heartbeat = false;
	}
	
	protected void startHB(){
		this.heartbeat = true;
		this.sendRIOPacket(Protocol.TXN, new TXNPacket(TXNProtocol.HB, 0, new byte[0]).pack());
	}
	
	protected void stopHB(){
		this.heartbeat = false;
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
		RIOPacket pkt = new RIOPacket(protocol, lastSeqNumSent + 1, payload, sessionID);
		
		if(establishingSession){			//Connection establishing a session
			this.queuedCommands.add(pkt);
		}else if(this.sessionID == -1){		//Connection needs to establish a session
			this.queuedCommands.add(pkt);
			this.establishSession();
		}else{								//Session is already set up. Proceed normally.
			byte[] packed = pkt.pack();
			if(packed.length > RIOPacket.MAX_PACKET_SIZE){
				System.out.println(DistNode.buildErrorString(this.destAddr, this.n.addr, protocol, "", Error.ERR_30));
			}else{
				lastSeqNumSent++;
				this.createTimeoutListener(pkt);
				n.send(destAddr, Protocol.TXN, packed);
			}
		}
	}
	
	/**
	 * Sends the server an ESTB_SESSION packet.<br>
	 * Sets the state of this OutChannel to cache commands while
	 * the session is being established.
	 */
	public void establishSession(){
		this.lastSeqNumSent = -1;
		this.establishingSession = true;
		SessionPacket sPkt = new SessionPacket(SessionProtocol.ESTB_SESSION, new byte[0]);
		RIOPacket pkt = new RIOPacket(Protocol.SESSION, -1, sPkt.pack(), -1);
		this.createTimeoutListener(pkt);
		n.send(destAddr, Protocol.SESSION, pkt.pack());
	}
	
	private void createTimeoutListener(RIOPacket pkt) {
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
		Integer numRetries = pktRetries.get( seqNum );
		if(unACKedPackets.containsKey(seqNum) && ( numRetries == null || numRetries < ReliableInOrderMsgLayer.MAX_RETRY ) ) {

			numRetries = numRetries == null ? 0 : numRetries;
			pktRetries.put( seqNum, numRetries + 1 );
			resendRIOPacket(n, seqNum);
		} else if(unACKedPackets.containsKey(seqNum)){
			RIOPacket pkt = unACKedPackets.remove(seqNum);
			pktRetries.remove(seqNum);
			
			if(pkt.getProtocol() == Protocol.SESSION)
				establishSession();
			else{
				boolean lastSequence = false;
				int maxSeqNum = seqNum + 1;
				while(!lastSequence){
					if(unACKedPackets.containsKey(maxSeqNum)){
						this.queuedCommands.add(unACKedPackets.remove(maxSeqNum));
						for(int i = 0; i < this.queuedCommands.size() - 1; i++)
							this.queuedCommands.add(this.queuedCommands.poll());
						pktRetries.remove(maxSeqNum);
						maxSeqNum++;
					} else
						lastSequence = true;
				}
				parent.TXNLayer.onRIOTimeout(this.destAddr, pkt.getPayload());
				if(!this.establishingSession){
					establishSession();
					this.n.printError("Node " + this.n.addr + ": Delay: Session timed out with node " + this.destAddr + ", establishing new session.");
				}
			}
		}
	}
	
	/**
	 * Called when we get an ACK back. Removes the outstanding packet if it is
	 * still in unACKedPackets. 
	 * 
	 * @param seqNum
	 *            The sequence number that was just ACKed
	 */
	protected RIOPacket receiveAck(int seqNum) {
		this.pktRetries.remove(seqNum);
		RIOPacket p = unACKedPackets.remove(seqNum);
		if(p != null){
			TXNPacket pkt = TXNPacket.unpack(p.getPayload());
			if(pkt != null && pkt.getProtocol() == TXNProtocol.HB && heartbeat)
				this.sendRIOPacket(Protocol.TXN, p.getPayload());
			return p;
		}
		return null;
	}
	
	/**
	 * Called to set up session on client side, after server has responded with sessionID and current seqNum for given sessionID.
	 * Also sets state of this connection to be not setting up session and sends commands that were queued while the session was being established.
	 * Does nothing if the session is already set.
	 * @param sessionId set sessionId to for this connection
	 * @param seqNum set lastSeqNumSent to for this connection (could be not initial value in case of client failure)
	 */
	protected void receiveAckSession(int sessionId, int seqNum) {
		this.establishingSession = false;
		this.unACKedPackets = new HashMap<Integer, RIOPacket>();
		this.sessionID = sessionId;

		this.lastSeqNumSent = seqNum;
		
		while(!this.queuedCommands.isEmpty()){
			RIOPacket p = this.queuedCommands.poll();
			if(p.getSeqNum() > seqNum)
				this.sendRIOPacket(p.getProtocol(), p.getPayload());
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
			//System.out.println("Values: " + toS(this.unACKedPackets.values()) + " Keys: " + toS(this.unACKedPackets.keySet()));
			//System.out.println("SeqNum: " + seqNum + " Protocol: " + pkt.getProtocol());
			
			n.send(destAddr, pkt.getProtocol(), pkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, seqNum }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static <E> String toS(Collection<E> c){
		String rtn = "";
		for(E e : c){
			rtn += e.toString() + ", ";
		}
		return rtn;
	}
}

