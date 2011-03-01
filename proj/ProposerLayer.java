import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;


public class ProposerLayer {

	private PaxosLayer paxosLayer;
	private int promises;
	private int rejects;
	private static int MAJORITY;
	private int proposalNumber;
	private int instanceNumber;
	private DistNode n;
	private HashMap<String, Integer> instanceValues;
	private HashMap<Integer, String> values;
	private Queue<String> commits = new LinkedList<String>();
	

	public ProposerLayer(PaxosLayer paxosLayer) {
		//this.paxosLayer = paxosLayer;
		ProposerLayer.MAJORITY = PaxosLayer.ACCEPTORS.length / 2 + 1;
		this.proposalNumber = 0;

		n = paxosLayer.n;
		this.instanceNumber = fillGaps();
		this.promises = 0;
		this.rejects = 0;
	}
	
	public void start(PaxosLayer paxosLayer){

		this.instanceNumber = fillGaps();

	}
	
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}

	public void receivedPromise(int from, PaxosPacket pkt) {
		int propNumber = pkt.getProposalNumber();
		if(propNumber > this.proposalNumber){
			this.proposalNumber = propNumber;
			this.values.put(pkt.getInstanceNumber(), Utility.byteArrayToString(pkt.getPayload()));
		}
		
		if(pkt.getProtocol() == PaxosProtocol.REJECT){
			rejects++;
			if(rejects >= MAJORITY){
				sendPrepares();
				resetRP();
			}

		} else {
			promises++;
			if(promises >= MAJORITY){
				sendProposal();
				resetRP();
			}
		}
			
	}
	
	private void resetRP(){
		this.promises = 0;
		this.rejects = 0;
	}
	
	
	public void receivedRecovery(int from, PaxosPacket pkt){
		if(pkt.getProtocol() == PaxosProtocol.RECOVERY_CHOSEN){

			paxosLayer.getLearnerLayer().writeValue(new Proposal(pkt));
			
		} else{
			String payload = Utility.byteArrayToString(pkt.payload);
			if(instanceValues.containsKey(payload)){
				int count = instanceValues.get(payload) + 1;
				if(count >= MAJORITY){
					paxosLayer.getLearnerLayer().writeValue(new Proposal(pkt));
				} else 
					instanceValues.put(payload, count);
			}else
				instanceValues.put(payload, 1);
		}
		
	}
	
	private void sendProposal() {
		PaxosPacket pkt = new PaxosPacket(PaxosProtocol.PROPOSE, this.proposalNumber, 
				this.instanceNumber, Utility.stringToByteArray(this.values.get(this.instanceNumber)));
		for(int acceptor : PaxosLayer.ACCEPTORS){
			send(acceptor, pkt);
		}
	}

	public void receivedCommit(int from, String commit){
		commits.add(commit);
		if(commits.size() == 1)
			newInstance(null);
		
	}
	
	private void newInstance(String value){
		resetRP();
		if(value != null)
			this.values.put(this.instanceNumber, value);
		if(commits.size() > 0){
			if(value == commits.peek())
				commits.remove();
	
			this.instanceNumber++;
			this.values.put(this.instanceNumber, commits.peek());
		}	
		
	}
	
	private void fixHole(int instance){
		for(int acceptor : PaxosLayer.ACCEPTORS){
			PaxosPacket pkt = new PaxosPacket(PaxosProtocol.RECOVERY, 0, instance, null);
			send(acceptor, pkt);
		}		
	}
	
	/**
	 * Sends a prepare request to all known acceptors with the highest proposal number we have seen for the current instanceNumber
	 * and nothing in the payload
	 */
	private void sendPrepares(){
		for(int acceptor : PaxosLayer.ACCEPTORS){
			PaxosPacket pkt = new PaxosPacket(PaxosProtocol.PREPARE, this.proposalNumber, this.instanceNumber, null);
			send(acceptor, pkt);
		}
	}
	
	/**
	 * 
	 * @return the current instance of paxos you should be using
	 */
	private int fillGaps(){
			
			ArrayList<Integer> missingInst = paxosLayer.getLearnerLayer().getMissingInstanceNums();
			for(Integer instance : missingInst)
				fixHole(instance);
			int largestInst = paxosLayer.getLearnerLayer().getLargestInstanceNum();


		return largestInst;
	}


	
	
}
