import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;


public class ProposerLayer {

	private PaxosLayer paxosLayer;
	private int promises;
	private int majority;
	private Queue<Commit> commits = new LinkedList<Commit>();
	private int proposalNumber;
	private int instanceNumber;
	private DistNode n;
	

	public ProposerLayer(PaxosLayer paxosLayer) {
		this.paxosLayer = paxosLayer;
		this.promises = 0;
		this.majority = PaxosLayer.ACCEPTORS.length / 2 + 1;
		this.proposalNumber = 0;
		//TODO: fill gaps!!!, read from disk!!!
		n = paxosLayer.n;
		if(n.fileExists(".pInstances"))
			this.instanceNumber = fillGaps();
		else{
			//TODO: Create File
		}
		
	}
	
	public void send(int dest, PaxosPacket pkt){
		this.paxosLayer.send(dest, pkt);
	}

	public void receivedPromise(int from, PaxosPacket pkt) {
		//TODO: log promise?
		promises++;
		if(promises >= majority){
			PaxosPacket proposal = createProposal();
			for(int acceptor : PaxosLayer.ACCEPTORS)
				send(acceptor, proposal);
		}
			
	}
	
	private PaxosPacket createProposal() {
		// TODO shouldn't pass empty byte arr, should be value
		return new PaxosPacket(PaxosProtocol.PROPOSE, this.proposalNumber, this.instanceNumber, new byte[0]);
	}

	public void recievedCommit(int from, Commit commit){
		if(commit.isWaiting())
			commits.add(commit);
		else if(!commit.abort()){
			//send prepare
		} else {
			n.TXNLayer.send(from, TXNProtocol.ABORT, null);			
		}
			
	}
	
	public void fixHole(int instance){
		
	}
	
	/**
	 * 
	 * @return returns the last instance of Paxos + 1
	 */
	public int fillGaps(){
		try {

			PersistentStorageReader r = n.getReader(".pInstances");
			String s = r.readLine();
			int counter = 0;
			while(s != null){
				int current = Integer.parseInt(s);
				if(current != counter){
					//TODO: FIX HOLE
					while(counter != current){
						fixHole(counter);
						counter++;
					}
					
				}
				counter = current;;
				s = r.readLine();
				
			}
			return counter;


		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}


	
	
}
