import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

public class ProposerLayer {

	private PaxosLayer paxosLayer;
	private int promises;//promises received for current instance
	private int rejects;//rejects received for current instance
	private static int MAJORITY;
	private int proposalNumber;//max proposal number for current instance
	private int instanceNumber;//current instance
	/**
	 * instance num -> number of recovery responses returned
	 */
	private Map<Integer, Map<String, Integer>> instanceValues;//maps instance number to value to 
															  //number of times that value was received
															  //in recovery
	private Map<Integer, String> values;
	private Queue<String> commits;//commits received
	private int holes;//number of holes found at new instance
	private int fixedHoles;//number of holes fixed
	private DistNode n;
	/**
	 * stores instances that aren't finished on recovery
	 */
	private Queue<Integer> unFinishedInstances;

	public ProposerLayer(PaxosLayer paxosLayer, DistNode n) {
		this.commits = new LinkedList<String>();
		this.instanceValues = new HashMap<Integer, Map<String, Integer>>();
		this.values = new HashMap<Integer, String>();
		this.unFinishedInstances = new LinkedList<Integer>();
		
		this.n = n;
		
		this.paxosLayer = paxosLayer;
		ProposerLayer.MAJORITY = PaxosLayer.ACCEPTORS.length / 2 + 1;
		this.proposalNumber = 0;
		this.instanceNumber = -1;

		this.promises = 0;
		this.rejects = 0;
	}
	
	public void start(){
		//this.instanceNumber = fillGaps();
	}
	
	/**
	 * sends the passed packet to the passed destination
	 * @param destination
	 * @param pkt to send
	 */
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}

	/**
	 * Used for handling REJECT protocol and PROMISE protocol and keeping track
	 * of max proposal number and value pairs
	 * If # of rejects is larger then majority, then resend prepares
	 * If # of promises is larger then majority, send proposal
	 * @param from
	 * @param pkt
	 */
	public void receivedPromise(int from, PaxosPacket pkt) {
		if(pkt.getInstanceNumber() == this.instanceNumber && this.values.containsKey(this.instanceNumber)){
			int propNumber = pkt.getProposalNumber();
			if(propNumber > this.proposalNumber){
				this.proposalNumber = propNumber;
				this.values.put(pkt.getInstanceNumber(), Utility.byteArrayToString(pkt.getPayload()));
			}
			
			if(pkt.getProtocol() == PaxosProtocol.REJECT){
				rejects++;
				if(rejects >= MAJORITY){
					sendPrepares(this.instanceNumber);
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
	}
	
	/**
	 * resets the rejects and promises to 0
	 */
	private void resetRP(){
		this.promises = 0;
		this.rejects = 0;
	}
	
	/**
	 * Handles receiving a recovery packet for a given instance
	 * If it is a 'chosen' packet, we learn that value
	 * If the majority has responded to that instance with a 'accepted' packet
	 * and the same value, we learn that value
	 * If the majority cannot come to a consensus, we finish that instance
	 * @param from
	 * @param pkt
	 */
	public void receivedRecovery(int from, PaxosPacket pkt){
		if(pkt.getProtocol() == PaxosProtocol.RECOVERY_CHOSEN){
			fixedHoles++;
			paxosLayer.getLearnerLayer().writeValue(new Proposal(pkt));
			this.instanceValues.remove(pkt.getInstanceNumber());
			
		} else{
			Proposal p = new Proposal(pkt);
			if(instanceValues.containsKey(p.getInstanceNum())){
				int count = instanceValues.get(p.getInstanceNum()).get(p.getValue()) + 1;
				int totalRtnedForInst = reduce(this.instanceValues.get(p.getInstanceNum()).values());
				int max = max(this.instanceValues.get(p.getInstanceNum()).values());
				
				if(count >= MAJORITY){
					fixedHoles++;
					paxosLayer.getLearnerLayer().writeValue(p);
					this.instanceValues.remove(p.getInstanceNum());
				}else if(totalRtnedForInst > PaxosLayer.ACCEPTORS.length / 2 + 1 && 
						PaxosLayer.ACCEPTORS.length / 2 < (totalRtnedForInst - max)){
					
					this.unFinishedInstances.add(pkt.getInstanceNumber());
					
					String maxValue = null;
					for(String s : this.instanceValues.get(pkt.getInstanceNumber()).keySet())
						if(this.instanceValues.get(pkt.getInstanceNumber()).containsKey(max))
							maxValue = s;
					
					if(maxValue != null)
						this.newRecoveryInstance(pkt.getInstanceNumber(), maxValue);
					else
						this.instanceValues.remove(pkt.getInstanceNumber());
					
				}else 
					instanceValues.get(p.getInstanceNum()).put(p.getValue(), count);
			}else{
				instanceValues.put(p.getInstanceNum(), new HashMap<String, Integer>());
				this.instanceValues.get(p.getInstanceNum()).put(p.getValue(), 1);
			}
		}
		//DONE FIXING HOLES IF THIS IS TRUE, READY TO START A NEW INSTANCE!!
		if(fixedHoles == holes && !this.commits.isEmpty() && this.unFinishedInstances.isEmpty()){
			newInstance(commits.peek());
			this.instanceValues.clear();
		}
		
	}
	
	/**
	 * Takes a collection of Integers and returns the max
	 * @param coll
	 * @return the max from the passed collection
	 */
	private static int max(Collection<Integer> coll){
		int max = -1;
		for(Integer i : coll)
			if(i > max)
				max = i;
		return max;
	}
	
	/**
	 * Takes a collection and returns the sum
	 * @param coll
	 * @return the sum of all Integers in the passed collection
	 */
	private static int reduce(Collection<Integer> coll){
		int rtn = 0;
		for(Integer i : coll)
			rtn += i;
		return rtn;
	}
	
	/**
	 * Sends a proposal message to all acceptors. Uses the current instance number
	 * and the current value
	 */
	private void sendProposal() {
		PaxosPacket pkt = new PaxosPacket(PaxosProtocol.PROPOSE, this.proposalNumber, 
				this.instanceNumber, Utility.stringToByteArray(this.values.get(this.instanceNumber)));
		for(int acceptor : PaxosLayer.ACCEPTORS){
			send(acceptor, pkt);
		}
	}

	/**
	 * Adds the passed commmit to the commit queue
	 * @param commit
	 */
	public void receivedCommit(String commit){
		commits.add(commit);
		if(commits.size() == 1){
			newInstance(commit);
		}
	}
	
	/**
	 * Takes a learned value and tells you that that value was learned for the current instance
	 * Now we can move onto the next commit and start a new instance of paxos
	 * @param learnedValue
	 */
	public void instanceFinished(String learnedValue){
		this.values.remove(this.instanceNumber);
		this.instanceValues.remove(this.instanceNumber);
		
		if(!this.unFinishedInstances.isEmpty()){
			//TODO: check for more holes, and maybe start new instance?
		}else{
			String commit = commits.poll();
			
			if(commit != null){
				int learnedID = Transaction.getIdFromString(learnedValue);
				int txnID = Transaction.getIdFromString(commit);
				if(learnedID == txnID)
					commit = commits.poll();
				if(commit != null)
					this.receivedCommit(commit);
			}
		}
	}
	
	/**
	 * Starts a new instance of Paxos
	 * @param value
	 */
	private void newInstance(String value){
		resetRP();
		this.proposalNumber = 0;
		this.instanceNumber = fillGaps() + 1;
		this.createTimeoutListener(this.instanceNumber, false);
	}
	
	/**
	 * Starts a new instance for a recovery instance that never finished
	 * @param instanceNum
	 * @param value
	 */
	private void newRecoveryInstance(int instanceNum, String value){
		resetRP();
		this.instanceNumber = instanceNum;
		this.values.put(instanceNum, value);
		this.sendPrepares(instanceNum);
		//TODO: double check
	}
	
	/**
	 * Sends recovery messages to all acceptors for the missing instance
	 * @param instance
	 */
	private void fixHole(int instance){
		this.createTimeoutListener(instance, true);
		for(int acceptor : PaxosLayer.ACCEPTORS){
			PaxosPacket pkt = new PaxosPacket(PaxosProtocol.RECOVERY, 0, instance, new byte[0]);
			send(acceptor, pkt);
		}
	}
	
	/**
	 * Sends a prepare request to all known acceptors with the highest proposal number we have seen for the current instanceNumber
	 * and nothing in the payload
	 */
	private void sendPrepares(int instanceNumber){
		for(int acceptor : PaxosLayer.ACCEPTORS){
			PaxosPacket pkt = new PaxosPacket(PaxosProtocol.PREPARE, this.proposalNumber, instanceNumber, new byte[0]);
			send(acceptor, pkt);
		}
	}
	
	/**
	 * 
	 * @return the current instance of paxos you should be using
	 */
	private int fillGaps(){
		ArrayList<Integer> missingInst = paxosLayer.getLearnerLayer().getMissingInstanceNums();
		int largestAccInst = paxosLayer.getAcceptorLayer().getMaxInstanceNumber();
		int largestLearnInst = paxosLayer.getLearnerLayer().getLargestInstanceNum();
		int largestInst = Math.max(paxosLayer.getAcceptorLayer().getMaxInstanceNumber(), paxosLayer.getLearnerLayer().getLargestInstanceNum());
		
		for(int i = largestLearnInst + 1; i < largestAccInst + 1; i++)
			missingInst.add(i);
		
		if(missingInst.size() != 0){
			fixedHoles = 0;
			holes = missingInst.size();
			for(Integer instance : missingInst)
				fixHole(instance);
		}else if(!this.commits.isEmpty()){
			sendPrepares(largestInst + 1);
			this.values.put(largestInst + 1, commits.peek());
		}

		return largestInst;
	}
	
	/**
	 * Timeout listener created on current instance
	 * @param instance - Instance for this timeout
	 * @param isRecovery - True if it is a recovery timer. False otherwise
	 */
	private void createTimeoutListener(int instance, boolean isRecovery) {
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", this, new String[]{ "java.lang.Integer", "java.lang.Boolean"});
			//waits 13 time steps
			this.n.addTimeout(new Callback(onTimeoutMethod, this, new Object[]{ instance, isRecovery }), 13);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Timeout called after 13 steps on a listener. If it was listening to a recovery and it has not learned yet,
	 * we try fixing that hole again. Otherwise we do nothing
	 * If it is an instance timer and we are still on that isntance, we start taht instance over again and set a new
	 * timoutlistenr. 
	 * @param instance
	 * @param isRecovery
	 */
	public void onTimeout(Integer instance, Boolean isRecovery) {
		//it timed out, time to resend packets!
		if(isRecovery && !this.paxosLayer.getLearnerLayer().isLearned(instance)){
			fixHole(instance);
		}else if(this.instanceNumber == instance && this.paxosLayer.getLearnerLayer().getLargestInstanceNum() < instance){
			this.createTimeoutListener(instance, false);
			if(promises > MAJORITY)
				sendProposal();
			else
				sendPrepares(this.instanceNumber);
		}
	}



	
	
}
