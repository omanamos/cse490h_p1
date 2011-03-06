import edu.washington.cs.cse490h.lib.Utility;


public class Proposal implements Comparable<Proposal> {
	private int instanceNum;
	private int proposalNum;
	private String value;
	
	public Proposal( PaxosPacket p ) {
		this( p.getInstanceNumber(), p.getProposalNumber(), Utility.byteArrayToString( p.getPayload() ) );
	}
	
	public Proposal( int instanceNum, int proposalNum, String value ) {
		this.instanceNum = instanceNum;
		this.proposalNum = proposalNum;
		this.value = value;
	}
	
	public int getInstanceNum() {
		return this.instanceNum;
	}
	
	public int getProposalNum() {
		return this.proposalNum;
	}
	
	public String getValue() {
		return this.value;
	}
	
	public void setValue( String value ) {
		this.value = value;
	}
	
	public PaxosPacket getPaxosPacket( int protocol ) {
		return new PaxosPacket( protocol, this.proposalNum, this.instanceNum, Utility.stringToByteArray(this.value) );
	}
	
	public String toString() {
		return this.instanceNum + "|" + this.proposalNum + "|" + this.value;
	}

	public int compareTo(Proposal o) {
		if( this.instanceNum > o.getInstanceNum() ) {
			return 1;
		} else if( this.instanceNum < o.getInstanceNum() ) {
			return -1;
		} else {
			if( this.proposalNum > o.getProposalNum() ) {
				return 1;
			} else if( this.proposalNum < o.getProposalNum() ) {
				return -1;
			}
		}
		return 0;
	}
}
