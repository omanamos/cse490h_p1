
public class TXNProtocol extends Protocol {
	public static final int WQ = 6;
	public static final int WD = 7;
	public static final int WF = 8;
	public static final int COMMIT = 9;
	public static final int ABORT = 10;
	
	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int p) {
		return Protocol.isPktProtocolValid(p) || isTXNProtocol(p);
	}
	
	public static boolean isTXNProtocol(int p){
		return p == WQ || p == WD || p == WF || p == COMMIT || p == ABORT;
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
		case WQ: return "write query";
		case WD: return "write data";
		case WF: return "write forward";
		case COMMIT: return "commit";
		case ABORT: return "ABORT";
		default:
			return Protocol.protocolToString(protocol);
		}
	}
}
