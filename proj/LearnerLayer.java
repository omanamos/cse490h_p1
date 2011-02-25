import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import javax.swing.text.Utilities;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class LearnerLayer {
	public static final String LEARN_FILE = ".learned";
	
	private PaxosLayer paxosLayer;
	private HashMap<Integer, HashMap<Integer, Integer>> proposals; 
	private Set<Integer> learned;
	private DistNode n;
	
	public LearnerLayer(PaxosLayer paxosLayer) {
		this.paxosLayer = paxosLayer;
		this.n = this.paxosLayer.n;
		//read in shit from disk
		this.startup();
	}
	
	public void startup() {
		this.readLog();
	}
	
	public void readLog() {
		try {
			PersistentStorageReader r = this.n.getReader(LEARN_FILE);
			String line = r.readLine();
			while( line != null ) {
				String[] entry = line.split("|");
				
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void send(int dest, PaxosPacket pkt){
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
	
	public void receivedLearn(int from, PaxosPacket pkt){
		
	}
	
	public void receivedAccept(int from, PaxosPacket pkt){
		
		
		//current number of the instance/proposal number we have seen so far
		Integer pcount = proposals.get(pkt.getInstanceNumber()).get(pkt.getProposalNumber());
		if( pcount == null ) {
			pcount = 1;
		} else {
			pcount++;
		}
		proposals.get(pkt.getInstanceNumber()).put(pkt.getProposalNumber(), pcount);
		
		//if we have the majority then do stuff
		if( pcount >= PaxosLayer.ACCEPTORS.length / 2 ) {
			writeToLog( pkt );
			sendChosenToAllLearners( pkt );
		}
		
	}
	
	public void writeToLog( PaxosPacket pkt ) {
		int instanceNum = pkt.getInstanceNumber();
		int proposalNum = pkt.getProposalNumber();
		String value = Utility.byteArrayToString( pkt.getPayload() );
		
		try {
			PersistentStorageWriter w = this.n.getWriter(LEARN_FILE, true);
			w.write(instanceNum + "|" + proposalNum + "|" + value);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void sendChosenToAllLearners( PaxosPacket pkt ) {
		
	}
}
