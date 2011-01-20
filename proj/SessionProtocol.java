
/**
 *	These protocols are used to establish client - server sessions.
 */
public class SessionProtocol extends Protocol{
	public static final int ACK_SESSION = 4;
	public static final int EXPIRED_SESSION = 5;
	
	public static final int ESTB_SESSION = 6;
	
	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int p) {
		return Protocol.isPktProtocolValid(p) || isSessionProtocol(p);
	}
	
	public static boolean isSessionProtocol(int p){
		return p == ACK_SESSION || p == EXPIRED_SESSION || p == ESTB_SESSION;
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
			case ACK_SESSION:
				return "RIO Session Acknowledgement Packet";
			case EXPIRED_SESSION:
				return "RIO Expired Session Packet";
			case ESTB_SESSION:
				return "RIO Establish Session Packet";
			default:
				return Protocol.protocolToString(protocol);
		}
	}
}
