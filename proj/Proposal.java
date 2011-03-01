import edu.washington.cs.cse490h.lib.Utility;


public class Proposal {
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
	
	public PaxosPacket getPaxosPacket( int protocol ) {
		return new PaxosPacket( protocol, this.proposalNum, this.instanceNum, Utility.stringToByteArray(this.value) );
	}
	
	public String toString() {
		return this.instanceNum + "|" + this.proposalNum + "|" + this.value;
	}
}
