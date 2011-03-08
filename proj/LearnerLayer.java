import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

public class LearnerLayer {
	public static final String LEARN_FILE = ".learned";
	public static final String OUT_OF_ORDER = ".outOfOrder";
	
	private PaxosLayer paxosLayer;
	private HashMap<Integer, Proposal> proposals; //these are accepted proposals that haven't been processed, instance num, pro num, transaction string
	private HashMap<Integer, HashMap<Integer, Integer>> proposalCount; //instance num, proposal num, proposal count
	private HashMap<Integer, Proposal> outOfOrder; //all out of order proposals
	
	private HashMap<Integer, Proposal> learned; //instance num, proposal num
	private DistNode n;
	private int lastContInstance;  //The largest continuous instance number we have learned
	private int largestInstanceNum; //
	
	/**
	 * Creates a new learner layer
	 * @param paxosLayer
	 * @param n
	 */
	public LearnerLayer(PaxosLayer paxosLayer, DistNode n) {
		this.paxosLayer = paxosLayer;
		this.n = n;
		this.lastContInstance = -1;
		this.largestInstanceNum = -1;
		
		this.proposals = new HashMap<Integer, Proposal>();
		this.proposalCount = new HashMap<Integer, HashMap<Integer,Integer>>();
		this.outOfOrder = new HashMap<Integer, Proposal>();
		this.learned = new HashMap<Integer, Proposal>();
	}
	
	/**
	 * Called by the paxos layer. Must be called before the
	 * proposer layer is started.
	 */
	public void start() {
		this.readLog();
		this.readOutOfOrder();
	}
	
	public boolean isLearned(int instanceNum){
		return this.learned.containsKey(instanceNum);
	}
	
	/**
	 * Updates the largest instance number seen
	 * @param instanceNum
	 */
	private void updateLargestInstanceNum( int instanceNum ) {
		if( instanceNum > this.largestInstanceNum ) {
			this.largestInstanceNum = instanceNum;
		}
	}
	
	/**
	 * Called by the paxos layer when either an accept 
	 * or learn message is received
	 * @param from
	 * @param pkt
	 */
	public void receive(int from, PaxosPacket pkt) {
		if( pkt.getProtocol() == PaxosProtocol.ACCEPT ) {
			//we are the distinguished learner
			this.receivedAccept(from, pkt);
		} else {
			//we are just a learner
			this.receivedLearn(from, pkt);
		}
	}
	
	/**
	 * Called when a learn message is received. Extracts a proposal 
	 * from the packet.
	 * @param from
	 * @param pkt
	 */
	private void receivedLearn(int from, PaxosPacket pkt){
		Proposal p = new Proposal(pkt);
		executeProposal( p );
	}
	
	/**
	 * Learns the given proposal if the instance number is one greater
	 * than the largest continuous instance number we have learned. If not, 
	 * adds the proposal to the out of order hash and writes the hash to disk
	 * @param p - given proposal
	 */
	private void executeProposal( Proposal p) {
		int iNum = p.getInstanceNum();
		
		proposals.put(iNum, p ); //add this proposal to our proposals...
		updateLargestInstanceNum(iNum);
		
		if( iNum == this.lastContInstance + 1 ) {
			this.lastContInstance++;
			//ok we can process this
			this.pushProposalValueToDisk(p);
			
			//add to our learned
			learned.put( iNum, p );
			this.outOfOrder.remove( iNum );
			writeToLearnedLog();
			writeOutOfOrder();
			
			this.paxosLayer.getProposerLayer().instanceFinished(p.getValue());
			
			//now see if we can execute more proposals 
			this.executeNextLearnedProposal();
		} else if( iNum > this.lastContInstance + 1 ) {
			//ok we are missing something so write this proposal to the out of order log
			this.outOfOrder.put( iNum, p );
			writeOutOfOrder();
		}
	}
	
	/**
	 * Called after a proposal has been learned, and will call execute on and
	 * proposals that were out of order that can now be executed
	 */
	private void executeNextLearnedProposal() {
		Proposal p = proposals.get(this.lastContInstance + 1); //the next higher instance
		if( p != null )
			executeProposal( p);
	}
	
	/**
	 * Writes the value in the proposal to disk using the transaction layer. Sets the value of the proposal
	 * to abort if the transaction layer was unable to commit the transaction
	 * @param p
	 */
	private void pushProposalValueToDisk( Proposal p ) {
		Transaction txn = Transaction.fromString( p.getValue(), this.paxosLayer.getTransactionLayer().cache );
		boolean committed = commit( txn, txn.willAbort );

		if( !committed ) {
			//replace value of this proposal with txid followed by ABORT
			p.setValue( txn.id + "#ABORT" );
		}
		
	}
	
	/**
	 * Calls the paxos finished method on the transaction layer with the given transaction
	 * @param txn
	 * @param abort
	 * @return whether or not commit was successful
	 */
	private boolean  commit( Transaction txn, boolean abort) {
		//call the transaction layer to see if we can commit
		return this.paxosLayer.getTransactionLayer().paxosFinished(txn, abort);
	}
	
	
	/**
	 * Leader methods
	 * 
	 * 
	 * 
	 */
	
	
	/**
	 * Sends the given proposal to all learners in a learn message
	 * Does not send a message to this current node
	 */
	private void sendChosenToAllLearners( Proposal p ) {
		int[] learners = PaxosLayer.LEARNERS;
		PaxosPacket learnPacket = new PaxosPacket(PaxosProtocol.LEARN, p.getProposalNum(), p.getInstanceNum(), Utility.stringToByteArray(p.getValue()));
		for( int learner : learners ) {
			if( learner != this.n.addr ) //do not send to ourself
				this.send(learner, learnPacket);
		}
	}
	
	/**
	 * Called when an accept message is received. If there is a majority for the instance - proposal number in
	 * given in the packet, then the learner will learn the proposal, otherwise the method just increments
	 * the count for the instance number and proposal number.
	 * @param from
	 * @param pkt
	 */
	private void receivedAccept(int from, PaxosPacket pkt){
		//current number of the instance/proposal number we have seen so far
		HashMap<Integer, Integer> instanceHash = proposalCount.get(pkt.getInstanceNumber());
		if( instanceHash == null ) {
			instanceHash = new HashMap<Integer, Integer>();
			proposalCount.put( pkt.getInstanceNumber(), instanceHash );
		}
		Integer pcount = instanceHash.get(pkt.getProposalNumber());
		if( pcount == null ) {
			pcount = 1;
		} else {
			pcount++;
		}
		//increment our proposer count
		proposalCount.get(pkt.getInstanceNumber()).put(pkt.getProposalNumber(), pcount);
		
		int majority;
		int numAcceptors = PaxosLayer.ACCEPTORS.length;
		if( numAcceptors % 2 == 1 ) {
			majority = numAcceptors / 2 + 1;
		} else {
			majority = numAcceptors / 2;
		}
		
		//if we have the majority then do stuff
		if( pcount >= majority ) {
			Proposal p = new Proposal( pkt );
			proposalCount.remove(p.getInstanceNum()); //not sure if we should remove from our counts.
			this.masterLearn( p );
		}
	}
	
	/**
	 * Takes a proposal and pushes it to disk as well as tells all the other learners
	 * @param p
	 */
	private void masterLearn( Proposal p ) {
		executeProposal(p);
		sendChosenToAllLearners( p );
	}
	
	/**
	 * Sends the given packet to the given destination
	 * @param dest
	 * @param pkt
	 */
	private void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}
	
	
	
	/**
	 * Methods used by proposer
	 */
	
	
	/**
	 * Returns the missing instance numbers. These are the instances
	 * that are missing between the largest continuous and largest instance number
	 * we have seen.
	 */
	public ArrayList<Integer> getMissingInstanceNums() {
		ArrayList<Integer> missing = new ArrayList<Integer>();
		for( int i = this.lastContInstance + 1; i < this.largestInstanceNum; i++ ) {
			Proposal p = this.proposals.get(i);
			if( p == null ) {
				missing.add(i);
			}
		}
		Collections.sort(missing);
		return missing;
	}
	
	/**
	 * Returns the largest instance number we have seen
	 * @return
	 */
	public int getLargestInstanceNum() {
		return this.largestInstanceNum;
	}
	
	public int getLargestContInstanceNum(){
		return this.lastContInstance;
	}
	
	/**
	 * If the given propsal has not been learned yet, learn the proposal
	 * @param p
	 */
	public void writeValue( Proposal p ){
		if(!this.learned.containsKey(p.getInstanceNum()))
			this.masterLearn(p);
	}
	
	
	/**
	 * Disk methods
	 * 
	 * 
	 */
	
	/**
	 * Reads the learned log and populates the learned hash
	 */
	private void readLog() {
		try {
			PersistentStorageReader r = this.n.getReader(LEARN_FILE);
			String line = r.readLine();
			while( line != null && !line.trim().isEmpty() ) {
				String[] entry = line.split("\\|");
				int instanceNum = Integer.parseInt(entry[0]);
				int proposalNum = Integer.parseInt(entry[1]);
				String value = entry[2];
				Proposal p = new Proposal( instanceNum, proposalNum, value );
				learned.put( instanceNum, p );
				updateLargestInstanceNum( instanceNum );
				this.lastContInstance = Math.max(this.lastContInstance, instanceNum);
				line = r.readLine();
			}
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
		} catch (IOException e) {
			//e.printStackTrace();
		}
	}

	/**
	 * Writes the learned hash to disk
	 */
	private void writeToLearnedLog() {
		String learnContent = "";
		for( Integer iNum : this.learned.keySet() ) {
			learnContent += this.learned.get( iNum ).toString() + "\n";
		}
		
		try {
			this.n.write(LEARN_FILE, learnContent, false, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Reads the out of order log from disk and populates the hash
	 */
	private void readOutOfOrder() {
		try {
			PersistentStorageReader r = this.n.getReader(OUT_OF_ORDER);
			String line = r.readLine();
			while( line != null && !line.trim().isEmpty() ) {
				String[] entry = line.split("\\|");
				int instanceNum = Integer.parseInt(entry[0]);
				int proposalNum = Integer.parseInt(entry[1]);
				String value = entry[2];
				Proposal p = new Proposal( instanceNum, proposalNum, value );
				proposals.put( instanceNum, p);
				outOfOrder.put( instanceNum, p);
				updateLargestInstanceNum( instanceNum );
				line = r.readLine();
			}
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
		} catch (IOException e) {
			//e.printStackTrace();
		}
	}
	
	/**
	 * Writes the out of order hash to disk
	 */
	private void writeOutOfOrder() {
		String outoforderContent = "";
		for( Integer iNum : this.outOfOrder.keySet() ) {
			Proposal p = this.outOfOrder.get( iNum );
			outoforderContent += p.toString() + "\n";
		}
		try {
			this.n.write(OUT_OF_ORDER, outoforderContent, false, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the learned value for the given instance number. 
	 * Returns null if there is no learned proposal
	 * @param instanceNum
	 * @return
	 */
	public String getLearnedForInstance(int instanceNum) {
		Proposal p = learned.get( instanceNum );
		if( p != null ) { 
			return p.getValue();
		}
		return null;
	}
	
	
	
}
