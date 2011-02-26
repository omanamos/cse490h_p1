/**
 * <pre>
 * Contains details about the recognized protocols
 * </pre>
 */
public class Protocol {
	// Protocols for the Reliable in-order message layer
	// These should be Packet protocols
	public static final int TXN = 0;
	public static final int ACK = 1;
	public static final int SESSION = 2;
	public static final int HB = 3;
	public static final int RTN = 4;
	
	// Protocols for Testing Reliable in-order message delivery
	// These should be RIOPacket protocols
	public static final int RIOTEST_PKT = 10;
	
	public static final int MAX_PROTOCOL = 127;

	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int p) {
		return (p == TXN || p == ACK ||  p == SESSION || p == HB || p == RTN);
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
			case TXN:
				return "RIO Remote Procedure Call Packet";
			case ACK:
				return "RIO Acknowledgement Packet";
			case RIOTEST_PKT:
				return "RIO Testing Packet";
			case HB:
				return "RIO Heartbeat";
			case RTN:
				return "RIO Return Packet";
			default:
				return "Unknown Protocol";
		}
	}
}
