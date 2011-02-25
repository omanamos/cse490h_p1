import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Callback;


public class TimeoutManager {
	private TransactionLayer txnLayer;
	private DistNode node;
	private int timeout;
	
	private int seqNum;
	/**
	 * seqNum -> packet
	 */
	private Map<Integer, TXNPacket> unRtned;

	public TimeoutManager(int timeout, DistNode node, TransactionLayer txnLayer){
		this.timeout = timeout;
		this.node = node;
		this.txnLayer = txnLayer;
		this.seqNum = 0;
		this.unRtned = new HashMap<Integer, TXNPacket>();
	}
	
	public int nextSeqNum(){
		return this.seqNum;
	}
	
	public void createTimeoutListener(TXNPacket pkt){
		try{
			this.unRtned.put(pkt.getSeqNum(), pkt);
			Method onTimeoutMethod = Callback.getMethod("onTimeout", txnLayer, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			this.node.addTimeout(new Callback(onTimeoutMethod, txnLayer, new Object[]{ seqNum }), this.timeout);
			seqNum++;
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
