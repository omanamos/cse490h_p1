package paxos;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import packets.PaxosPacket;
import protocols.PaxosProtocol;

import nodes.DistNode;

import transactions.Transaction;
import utils.Proposal;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

public class AcceptorLayer {

	private static final String ACCEPT_FILE = ".acceptor_record"; //every proposal that has been accepted
	private static final String PROMISE_FILE = ".promises";		  //every promised proposal number for a instance
		
	
	private PaxosLayer paxosLayer;
	
	private HashMap<Integer, Proposal> acceptorRecord; //instance num -> accepted proposal
	private HashMap<Integer, Integer> promised; //instance num -> highest promised or accepted proposal number
	private DistNode n;
	
	private int maxInstanceNumber; //The highest instance number seen so far
	
	/**
	 * Creates a new Acceptor Layer
	 * @param paxosLayer
	 * @param n
	 */
	public AcceptorLayer(PaxosLayer paxosLayer, DistNode n) {
		this.paxosLayer = paxosLayer;
		this.n = n;
		this.acceptorRecord = new HashMap<Integer, Proposal>();
		this.promised = new HashMap<Integer, Integer>();
		this.maxInstanceNumber = -1;
	}
	
	/**
	 * Start method called by the paxos layer, must be called before
	 * the proposal layer is started
	 */
	public void start() {
		this.readLogs();
	}
	
	
	/**
	 * Sends the given paxos packet to the given destination 
	 * using the paxos layer
	 * @param dest - packet desitination
	 * @param pkt - Paxos Packet
	 */
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}
	
	/**
	 * Returns the max instance number this acceptor
	 * has seen so far
	 * @return the max instance number
	 */
	public int getMaxInstanceNumber(){
		return this.maxInstanceNumber;
	}
	
	/**
	 * Called when receiving a prepare message.
	 * This method will send a promise or a reject message
	 * back to the leader depending on what has already been
	 * promised and/or accepted
	 * 
	 * @param from - Sending node
	 * @param pkt - Received packet
	 */
	public void receivedPrepare(int from, int seqNum, PaxosPacket pkt){
		
		int newProposalNum = pkt.getProposalNumber();
		
		Integer promisedValue = promised.get( pkt.getInstanceNumber() );
		
		Proposal acceptedProposal = acceptorRecord.get(pkt.getInstanceNumber());
		
		PaxosPacket response;
		if( promisedValue == null || newProposalNum > promisedValue ) {
			promised.put( pkt.getInstanceNumber(), pkt.getProposalNumber());
			if( acceptedProposal == null ) {
				response = new PaxosPacket(PaxosProtocol.PROMISE, pkt.getProposalNumber() - 1, pkt.getInstanceNumber(), new byte[0] );
			} else {
				response = acceptedProposal.getPaxosPacket(PaxosProtocol.PROMISE);
			}
			
			//write to disk before our response
			updateState();
			
		} else {
			//REJECTION OOOOHHHH BURRRRNNN
			response = reject(pkt.getInstanceNumber(), promisedValue, acceptedProposal, pkt.getCurVersion() );
		}
		
		//send the response
		response.setCurVersion(pkt.getCurVersion());
		this.paxosLayer.rtn(from, seqNum, response);
	}
	
	/**
	 * Called when a propose message is received. A accept or reject message is sent back
	 * to the leader depending on whether or not a proposal has been accepted for the
	 * instance specified in the packet.
	 * 
	 * @param from
	 * @param pkt
	 */
	public void receivedPropose(int from, PaxosPacket pkt) {
		Proposal acceptedProposal = acceptorRecord.get(pkt.getInstanceNumber());
		Integer promisedValue = promised.get( pkt.getInstanceNumber() );
		int accTxnId = Transaction.getIdFromString(Utility.byteArrayToString(pkt.getPayload()));
		int newTxnId = acceptedProposal == null ? -1 : Transaction.getIdFromString(acceptedProposal.getValue());
		
		if( acceptedProposal == null && ( promisedValue == null || pkt.getProposalNumber() >= promisedValue ) ) {
			acceptedProposal = new Proposal( pkt );
			acceptorRecord.put( pkt.getInstanceNumber(), acceptedProposal );
			promised.put( pkt.getInstanceNumber(), pkt.getProposalNumber() );
			this.maxInstanceNumber = Math.max(this.maxInstanceNumber, pkt.getInstanceNumber());
			updateState();
			
			this.send(from, acceptedProposal.getPaxosPacket(PaxosProtocol.ACCEPT));
			
		}else if(accTxnId == newTxnId) {
			this.send(from, acceptedProposal.getPaxosPacket(PaxosProtocol.ACCEPT));
		}else {
			//REJECTION DAMNNN
			this.send(from, reject(pkt.getInstanceNumber(), promisedValue, acceptedProposal, pkt.getCurVersion() ));
		}
	}
	
	/**
	 * Used to generate a reject paxos packet based on the given paramters
	 * The packet that is return will either have an empty value or the value
	 * of the accepted proposal if the given accepted proposal is not null
	 * 
	 * @param instanceNum
	 * @param promisedValue
	 * @param acceptedProposal
	 * @return a reject paxos packet
	 */
	public PaxosPacket reject(int instanceNum, int promisedValue, Proposal acceptedProposal, int curVersion) {
		PaxosPacket response;
		if( acceptedProposal == null ) {
			response = new PaxosPacket(PaxosProtocol.REJECT, promisedValue, instanceNum, new byte[0]);
		} else {
			response = acceptedProposal.getPaxosPacket(PaxosProtocol.REJECT);
		}
		response.setCurVersion(curVersion);
		return response;
	}

	/**
	 * Called after receiving a recovery message. The asks the learner layer for and learned proposals for
	 * the instance number in the packet, if there is a learned proposal, the method sends a recovery chosen
	 * message back to the leader. If no proposal has been learned, then the method sends a recovery accepted
	 * message back to the leader with the accepted proposal message for the instance number in the packet.
	 * Nothing happens if there has not been a learned or accepted proposal for the instance number.
	 * @param from
	 * @param pkt
	 */
	public void receivedRecovery(int from, PaxosPacket pkt) {
		int instanceNum = pkt.getInstanceNumber();
		
		String response = this.paxosLayer.getLearnerLayer().getLearnedForInstance( instanceNum );
		if( response == null ) {
			Proposal acceptedProposal = this.acceptorRecord.get(instanceNum);
			if( acceptedProposal != null ) {
				this.send(from, acceptedProposal.getPaxosPacket(PaxosProtocol.RECOVERY_ACCEPTED));
			} else {
				PaxosPacket recoveryReject = new PaxosPacket( PaxosProtocol.RECOVERY_REJECT, -1, instanceNum, new byte[0]);
				this.send(from, recoveryReject );
			}
			
		} else {
			PaxosPacket chosen = new PaxosPacket( PaxosProtocol.RECOVERY_CHOSEN, -1, instanceNum, Utility.stringToByteArray(response));
			this.send( from, chosen );
		}
	}
	
	
	/**
	 * Writes the acceptor record and promise hashes to disk,
	 * overwriting the files that already exist.
	 */
	private void updateState() {
		try {
			String fileContents = "";
			for( int instanceNum : acceptorRecord.keySet() ) {
				Proposal p = acceptorRecord.get(instanceNum);
				fileContents += p.toString() + "\n";
			}
			this.n.write(ACCEPT_FILE, fileContents, false, true);
			
			String promisedContents = "";
			for( int instanceNum : promised.keySet() ) {
				promisedContents += instanceNum + " " + promised.get( instanceNum ) + "\n";
			}
			this.n.write(PROMISE_FILE, promisedContents, false, true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Reads the acceptor record and promise logs on disk
	 * and populates the corresponding hash maps in memory
	 */
	private void readLogs() {
		PersistentStorageReader r;
		try {
			r = this.n.getReader(ACCEPT_FILE);
			String line = r.readLine();
			while( line != null && !line.trim().isEmpty() ) {
				String[] entry = line.split("\\|");
				Proposal p = new Proposal( Integer.parseInt(entry[0]), Integer.parseInt(entry[1]), entry[2] );
				this.acceptorRecord.put(p.getInstanceNum(), p);
				this.maxInstanceNumber = Math.max(this.maxInstanceNumber, p.getInstanceNum());
				line = r.readLine();
			}
			
			r = this.n.getReader(PROMISE_FILE);
			line = r.readLine();
			while( line != null && !line.trim().isEmpty() ) {
				String[] entry = line.split(" ");
				this.promised.put(Integer.parseInt(entry[0]), Integer.parseInt(entry[1]));
				line = r.readLine();
			}
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
		} catch (IOException e) {
			//e.printStackTrace();
		}
	}
	
	
	
}
