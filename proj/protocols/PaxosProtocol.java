package protocols;

public class PaxosProtocol {
	public static final int PREPARE = 6;
	public static final int PROMISE = 7;
	public static final int PROPOSE = 8;
	public static final int ACCEPT = 9;
	public static final int LEARN = 10;
	public static final int RECOVERY = 11;
	public static final int RECOVERY_ACCEPTED = 12;
	public static final int RECOVERY_CHOSEN = 13;
	public static final int REJECT = 14;
	public static final int ELECT = 15;
	public static final int RECOVERY_REJECT = 16;
	
	
	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int p) {
		return Protocol.isPktProtocolValid(p) || isPaxosProtocol(p);
	}
	
	public static boolean isPaxosProtocol(int p){
		return p == PREPARE || p == PROPOSE || p == ACCEPT || p == LEARN || p == PROMISE || 
		p == RECOVERY || p == RECOVERY_ACCEPTED || p == RECOVERY_CHOSEN || p == REJECT ||
		p == ELECT || p == RECOVERY_REJECT;
	}

	/**
	 * Returns a string representation of the given protocol. Can be used for
	 * debugging
	 * 
	 * @param protocol
	 *            The protocol whose string representation is desired
	 * @return The string representation of the given protocol.
	 *         "Unknown Protocol" if the protocol is not recognized
	 */
	public static String protocolToString(int protocol) {
		switch (protocol) {
			case PREPARE: return "prepare";
			case PROPOSE: return "propose";
			case ACCEPT: return "accept";
			case LEARN: return "learn";
			case PROMISE: return "promise";
			case RECOVERY: return "recovery";
			case RECOVERY_ACCEPTED: return "recovery accepted";
			case RECOVERY_CHOSEN: return "recovery chosen";
			case RECOVERY_REJECT: return "recovery reject";
			case REJECT: return "reject";
			case ELECT: return "elect";
		default:
			return Protocol.protocolToString(protocol);
		}
	}
}
