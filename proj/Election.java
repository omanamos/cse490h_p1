import java.util.PriorityQueue;

public class Election {
	
	private PriorityQueue<Pair<Integer, Integer>> proposals;
	private int majority;
	
	public Election(int totalNumNodes){
		this.proposals = new PriorityQueue<Pair<Integer,Integer>>();
		majority = totalNumNodes / 2 + 1;
	}
	
	public void propose(int addr, int instanceNum){
		this.proposals.add(new Pair<Integer,Integer>(addr, instanceNum));
	}
	
	public boolean hasMajority(){
		return this.proposals.size() >= majority;
	}
	
	public Pair<Integer,Integer> elect(){
		if(!hasMajority())
			throw new IllegalStateException("Haven't received responses from a majority yet!");
		return this.proposals.poll();
	}
	
	public String toString(){
		return "Received " + this.proposals.size() + "/" + this.majority + " responses back for the election.";
	}
}
