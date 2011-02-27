import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import javax.swing.text.Utilities;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class LearnerLayer {
	public static final String LEARN_FILE = ".learned";
	public static final String OUT_OF_ORDER = ".outOfOrder";
	
	private PaxosLayer paxosLayer;
	private HashMap<Integer, Proposal> proposals; //these are accepted proposals that haven't been processed, instance num, pro num, transaction string
	private HashMap<Integer, HashMap<Integer, Integer>> proposalCount; //instance num, proposal num, proposal count
	private HashMap<Integer, Integer> learned; //instance num, proposal num
	private DistNode n;
	private int lastContInstance;
	
	public LearnerLayer(PaxosLayer paxosLayer) {
		this.paxosLayer = paxosLayer;
		this.n = this.paxosLayer.n;
		this.lastContInstance = -1;
	}
	
	public void start() {
		this.readLog();
		this.readOutOfOrder();
	}
	
	private void readLog() {
		try {
			PersistentStorageReader r = this.n.getReader(LEARN_FILE);
			String line = r.readLine();
			while( line != null ) {
				String[] entry = line.split(" ");
				int instanceNum = Integer.parseInt(entry[0]);
				int proposalNum = Integer.parseInt(entry[1]);
				learned.put( instanceNum, proposalNum );
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void readOutOfOrder() {
	try {
		PersistentStorageReader r = this.n.getReader(OUT_OF_ORDER);
		String line = r.readLine();
		while( line != null ) {
			String[] entry = line.split("|");
			int instanceNum = Integer.parseInt(entry[0]);
			int proposalNum = Integer.parseInt(entry[1]);
			String value = entry[2];
			
			Proposal p = new Proposal( instanceNum, proposalNum, value );
			proposals.put( instanceNum, p);
		}
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
	
	private void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}
	
	public void receive(int from, PaxosPacket pkt) {
		if( pkt.getProtocol() == PaxosProtocol.ACCEPT ) {
			//we are the distinguished learner
			this.receivedAccept(from, pkt);
		} else {
			//we are just a learner
			this.receivedLearn(from, pkt);
		}
	}
	
	private void receivedLearn(int from, PaxosPacket pkt){
		learnProposal(pkt);
	}
	
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
			proposalCount.remove(pkt.getInstanceNumber()); //not sure if we should remove from our counts.
			writeToLog( pkt ); //since we are the distinguished learner, write to our log
			sendChosenToAllLearners( pkt );
		}
		
	}
	
	private void learnProposal( PaxosPacket pkt ) {
		Proposal p = new Proposal(pkt);
		proposals.put(pkt.getInstanceNumber(), p ); //add this proposal to our proposals...
		
		//if false that means there is a hole somewhere otherwise we learned it
		if( executeProposal(p) ) {
			learned.put( p.getInstanceNum(), p.getProposalNum() );
		} else {
			//not sure yet probably tell the proposer about the hole or something
		}
	}
	
	private boolean executeProposal( Proposal p ) {
		int iNum = p.getInstanceNum();
		if( iNum == this.lastContInstance + 1 ) {
			this.lastContInstance++;
			//ok we can process this
			
			//now see if we can execute more proposals 
			this.executeNextLearnedProposal();
		} else if( iNum > this.lastContInstance + 1 ) {
			//ok we are missing something so write this proposal to the out of order log
			writeOutOfOrder( p );
		}
		
		return true;
	}
	
	private void executeNextLearnedProposal() {
		Proposal p = proposals.get(this.lastContInstance + 1); //the next higher instance
		executeProposal( p );
	}
	
	private void writeToLog( PaxosPacket pkt ) {
		int instanceNum = pkt.getInstanceNumber();
		int proposalNum = pkt.getProposalNumber();
		
		try {
			PersistentStorageWriter w = this.n.getWriter(LEARN_FILE, true);
			w.write(instanceNum + " " + proposalNum );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void writeOutOfOrder( Proposal p ) {
		try {
			PersistentStorageWriter w = this.n.getWriter(OUT_OF_ORDER, true);
			w.write(p.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void sendChosenToAllLearners( PaxosPacket pkt ) {
		int[] learners = PaxosLayer.LEARNERS;
		PaxosPacket learnPacket = new PaxosPacket(PaxosProtocol.LEARN, pkt.getProposalNumber(), pkt.getInstanceNumber(), pkt.getPayload());
		for( int learner : learners ) {
			this.send(learner, learnPacket);
		}
	}
	
	
	public int learnedProposalNumForInstance( int instanceNum ) {
		return learned.get( instanceNum );
	}
	
	public ArrayList<Integer> getMissingInstanceNums() {
		ArrayList<Integer> missing = new ArrayList<Integer>();
		if( lastContInstance != -1 ) {
			
		}
		
		
			
		return missing;
	}
}
