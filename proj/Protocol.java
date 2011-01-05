/**
 * <pre>
 * Contains details about the recognized protocols
 * </pre>
 */
public class Protocol {
	// Protocols for the Reliable in-order message layer
	// These should be Packet protocols
	public static final int DATA = 0;
	public static final int ACK = 1;

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
	public static boolean isPktProtocolValid(int protocol) {
		return (protocol == DATA || protocol == ACK);
	}

	/**
	 * Tests if the given protocol is valid for a RIOPacket. Note that the
	 * current implementation of RIOPacket actually uses this to test validity
	 * of packets.
	 * 
	 * @param protocol
	 *            The protocol to be checked
	 * @return True if protocol is valid, else false
	 */
	public static boolean isRIOProtocolValid(int protocol) {
		return protocol == RIOTEST_PKT;
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
		case DATA:
			return "RIO Data Packet";
		case ACK:
			return "RIO Acknowledgement Packet";
		case RIOTEST_PKT:
			return "RIO Testing Packet";
		default:
			return "Unknown Protocol";
		}
	}
}
