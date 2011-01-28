
/**
 *	These protocols are sent by servers and received by clients.
 */
public class RTNProtocol extends Protocol {
	
	public static final int DATA = 6;
	public static final int ERROR = 7;
	public static final int WC = 8;
	public static final int RC = 9;
	public static final int IC = 10;
	
	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int p) {
		return Protocol.isPktProtocolValid(p) || isACKProtocol(p);
	}
	
	public static boolean isACKProtocol(int p){
		return p == DATA || p == ERROR;
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
			case DATA: return "data return";
			case ERROR: return "error";
			default:
				return Protocol.protocolToString(protocol);
		}
	}
}
