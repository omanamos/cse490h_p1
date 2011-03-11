package paxos;

import java.util.PriorityQueue;

import utils.Pair;


public class Election {
	
	private PriorityQueue<Pair<Integer, Integer>> proposals;
	private int majority;
	
	private int timedOut;
	
	public Election(int totalNumNodes){
		this.proposals = new PriorityQueue<Pair<Integer,Integer>>();
		this.majority = totalNumNodes / 2 + 1;
		this.timedOut = 0;
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
	
	public boolean onTimeout(){
		this.timedOut++;
		return this.timedOut >= this.majority;
	}
	
	public String toString(){
		return "Received " + this.proposals.size() + "/" + this.majority + " responses back for the election.";
	}
}
