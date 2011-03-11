package paxos;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import packets.PaxosPacket;
import protocols.PaxosProtocol;

import nodes.DistNode;

import transactions.Transaction;
import utils.Proposal;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

public class ProposerLayer {
	
	private static final int TIMEOUT = 13;

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
	/**
	 * instance num -> node addr -> has responded since last timeout/start
	 */
	private Map<Integer, Set<Integer>> recoveryResponses;
	private Map<Integer, String> values;
	private Queue<String> commits;//commits received
	private int holes;//number of holes found at new instance
	private int fixedHoles;//number of holes fixed
	private DistNode n;
	/**
	 * stores instances that aren't finished on recovery
	 */
	private Queue<Integer> unFinishedInstances;
	
	private int curVersion;

	public ProposerLayer(PaxosLayer paxosLayer, DistNode n) {
		this.commits = new LinkedList<String>();
		this.instanceValues = new HashMap<Integer, Map<String, Integer>>();
		this.values = new HashMap<Integer, String>();
		this.unFinishedInstances = new LinkedList<Integer>();
		this.recoveryResponses = new HashMap<Integer, Set<Integer>>();
		
		this.n = n;
		
		this.paxosLayer = paxosLayer;
		ProposerLayer.MAJORITY = PaxosLayer.ACCEPTORS.length / 2 + 1;
		this.proposalNumber = 0;
		this.instanceNumber = -1;
		this.curVersion = 0;

		this.promises = 0;
		this.rejects = 0;
	}
	
	public void start(){
		//TODO:this.instanceNumber = fillGaps();
		if(this.n.fileExists(".prepare_version")){
			try{
				this.curVersion = Integer.parseInt(this.n.get(".prepare_version"));
			}catch(Exception e){}
		}
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
		int propNumber = pkt.getProposalNumber() + 1;
		String value = Utility.byteArrayToString(pkt.getPayload());
		
		if(pkt.getInstanceNumber() == this.instanceNumber && this.values.containsKey(this.instanceNumber) && pkt.getCurVersion() == this.curVersion){
			
			if(propNumber > this.proposalNumber || this.values.get(pkt.getInstanceNumber()).isEmpty() || !value.isEmpty()){
				this.proposalNumber = Math.max(this.proposalNumber, propNumber);
				if(!value.isEmpty())
					this.values.put(pkt.getInstanceNumber(), value);
			}
			
			if(pkt.getProtocol() == PaxosProtocol.REJECT){
				rejects++;
				if(rejects >= MAJORITY){
					updateVersion();
					sendPrepares(this.instanceNumber);
					resetRP();
				}
	
			} else{
				promises++;
				if(promises >= MAJORITY){
					updateVersion();
					sendProposal();
					this.rejects = 0;
				}
			}
		}
	}
	
	private void updateVersion(){
		try{
			this.curVersion++;
			this.n.write(".prepare_version", this.curVersion + "", false, true);
		}catch(Exception e){}
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
		if(this.recoveryResponses.containsKey(pkt.getInstanceNumber()) && 
			!this.recoveryResponses.get(pkt.getInstanceNumber()).contains(from)){ //make sure we haven't heard from this node already
			
			this.recoveryResponses.get(pkt.getInstanceNumber()).add(from);
			
			if(pkt.getProtocol() == PaxosProtocol.RECOVERY_CHOSEN){
				fixedHoles++;
				paxosLayer.getLearnerLayer().writeValue(new Proposal(pkt));
				this.instanceValues.remove(pkt.getInstanceNumber());
				this.recoveryResponses.remove(pkt.getInstanceNumber());
				
			}else{
				Proposal p = new Proposal(pkt);
				if(instanceValues.containsKey(p.getInstanceNum())){
					if(!this.instanceValues.get(p.getInstanceNum()).containsKey(p.getValue()))
						this.instanceValues.get(p.getInstanceNum()).put(p.getValue(), 0);
					
					int count = instanceValues.get(p.getInstanceNum()).get(p.getValue()) + 1;
					this.instanceValues.get(p.getInstanceNum()).put(p.getValue(), count);
					
					int totalRtnedForInst = reduce(this.instanceValues.get(p.getInstanceNum()).values());
					int max = getMaxValue(this.instanceValues.get(p.getInstanceNum()));
					
					if(count >= MAJORITY && !p.getValue().isEmpty()){
						fixedHoles++;
						paxosLayer.getLearnerLayer().writeValue(p);
						this.instanceValues.remove(p.getInstanceNum());
						this.recoveryResponses.remove(p.getInstanceNum());
					}else if(totalRtnedForInst > PaxosLayer.ACCEPTORS.length / 2 + 1 && 
							PaxosLayer.ACCEPTORS.length / 2 < (totalRtnedForInst - max) ||
							count >= MAJORITY){
						
						this.unFinishedInstances.add(pkt.getInstanceNumber());
						
						if(this.unFinishedInstances.size() == 1){
							String maxValue = getMaxKey(this.instanceValues.get(pkt.getInstanceNumber()));
							
							if(maxValue != null)
								this.newRecoveryInstance(pkt.getInstanceNumber(), maxValue);
							else
								this.newRecoveryInstance(pkt.getInstanceNumber(), "");
						}
					}
				}else{
					instanceValues.put(p.getInstanceNum(), new HashMap<String, Integer>());
					this.instanceValues.get(p.getInstanceNum()).put(p.getValue(), 1);
				}
			}
			//DONE FIXING HOLES IF THIS IS TRUE, READY TO START A NEW INSTANCE!!
			if(fixedHoles == holes && !this.commits.isEmpty() && this.unFinishedInstances.isEmpty()){
				newInstance(commits.peek());
				this.instanceValues.clear();
				this.recoveryResponses.clear();
			}
		}
	}
	
	/**
	 * @param map
	 * @return the value with the maximum value
	 */
	private static int getMaxValue(Map<String, Integer> map){
		int max = -1;
		for(String s : map.keySet())
			if((max == -1 || map.get(s) > max) && !s.isEmpty())
				max = map.get(s);
		return max;
	}
	
	/**
	 * @param map
	 * @return the key with the maximum value
	 */
	private static String getMaxKey(Map<String, Integer> map){
		Integer max = null;
		String rtn = null;
		for(String s : map.keySet()){
			if((max == null || map.get(s) > max) && !s.isEmpty()){
				max = map.get(s);
				rtn = s;
			}
		}
		return rtn;
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
				this.instanceNumber, this.curVersion, Utility.stringToByteArray(this.values.get(this.instanceNumber)));
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
		this.recoveryResponses.remove(this.instanceNumber);
		
		if(!this.unFinishedInstances.isEmpty()){
			this.fixedHoles++;
			this.unFinishedInstances.poll();
			if(!this.unFinishedInstances.isEmpty()){
				Integer inst = this.unFinishedInstances.poll();
				String value = getMaxKey(this.instanceValues.get(inst));
				if(value != null)
					this.newRecoveryInstance(inst, value);
				else
					throw new IllegalStateException("Missing recovery instance value: code 1.");
			}else if(this.instanceValues.isEmpty() && !this.commits.isEmpty()){
				this.newInstance(this.commits.peek());
			}
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
	}
	
	/**
	 * Starts a new instance for a recovery instance that never finished
	 * @param instanceNum
	 * @param value
	 */
	private void newRecoveryInstance(int instanceNum, String value){
		resetRP();
		this.proposalNumber = 0;
		this.instanceNumber = instanceNum;
		this.values.put(instanceNum, value);
		this.sendPrepares(instanceNum);
	}
	
	/**
	 * Sends recovery messages to all acceptors for the missing instance
	 * @param instance
	 */
	private void fixHole(int instance){
		this.createTimeoutListener(instance, true);
		
		if(!this.instanceValues.containsKey(instance))
			this.instanceValues.put(instance, new HashMap<String,Integer>());
		
		if(!this.recoveryResponses.containsKey(instance))
			this.recoveryResponses.put(instance, new HashSet<Integer>());
		
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
			PaxosPacket pkt = new PaxosPacket(PaxosProtocol.PREPARE, this.proposalNumber, instanceNumber, this.curVersion, new byte[0]);
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
	 * @param isRunning - True if an instance of paxos is running (recovery or normal). false otherwise
	 */
	private void createTimeoutListener(int instance, boolean isRecovery) {
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", this, new String[]{ "java.lang.Integer", "java.lang.Boolean"});
			//waits 13 time steps
			this.n.addTimeout(new Callback(onTimeoutMethod, this, new Object[]{ instance, isRecovery }), TIMEOUT);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Timeout called after 13 steps on a listener. If it was listening to a recovery and it has not learned yet,
	 * we try fixing that hole again. Otherwise we do nothing
	 * If it is an instance timer and we are still on that instance, we start that instance over again and set a new
	 * timeout listener. 
	 * @param instance
	 * @param isRecovery
	 */
	public boolean onTimeout(Integer instance, Boolean isRecovery) {
		//it timed out, time to resend packets!
		if(isRecovery){
			if(!this.unFinishedInstances.contains(instance) && this.instanceValues.containsKey(instance) 
					&& !this.paxosLayer.getLearnerLayer().isLearned(instance)){
				fixHole(instance);
			}
			return false;
		}else{
			if(promises > MAJORITY)
				sendProposal();
			else
				return true;
			return false;
		}
	}
}
