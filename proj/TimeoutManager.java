import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Callback;


public class TimeoutManager {
	private TransactionLayer txnLayer;
	private DistNode node;
	private int timeout;
	
	//TODO: each connection has to have its own seqNum
	private int seqNum;
	/**
	 * addr -> session ID
	 */
	private Map<Integer, Integer> sids;
	/**
	 * addr -> seqNum -> packet
	 */
	private Map<Integer, Map<Integer, TXNPacket>> unRtned;

	public TimeoutManager(int timeout, DistNode node, TransactionLayer txnLayer){
		this.timeout = timeout;
		this.node = node;
		this.txnLayer = txnLayer;
		this.seqNum = 0;
		this.unRtned = new HashMap<Integer, Map<Integer, TXNPacket>>();
	}
	
	public void onRtn(int dest, int seqNum){
		this.unRtned.get(dest).remove(seqNum);
	}
	
	public void onTimeout(Integer dest, Integer seqNum){
		
	}
	
	public int nextSeqNum(){
		return this.seqNum;
	}
	
	public void createTimeoutListener(int dest, int sid, TXNPacket pkt){
		try{
			this.sids.put(dest, sid);
			this.addPkt(dest, pkt);
			Method onTimeoutMethod = Callback.getMethod("onTimeout", this, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			this.node.addTimeout(new Callback(onTimeoutMethod, txnLayer, new Object[]{ dest, pkt.getSeqNum() }), this.timeout);
			seqNum++;
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void addPkt(int dest, TXNPacket pkt){
		if(!this.unRtned.containsKey(dest))
			this.unRtned.put(dest, new HashMap<Integer, TXNPacket>());
		this.unRtned.get(dest).put(pkt.getSeqNum(), pkt);
	}
}
