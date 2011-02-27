import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Callback;


public class TimeoutManager {
	private TransactionLayer txnLayer;
	private DistNode node;
	private int timeout;
	
	/**
	 * addr -> seqNum
	 */
	private Map<Integer, Integer> seqNums;
	/**
	 * addr -> seqNum -> packet
	 */
	private Map<Integer, Map<Integer, TXNPacket>> unRtned;

	public TimeoutManager(int timeout, DistNode node, TransactionLayer txnLayer){
		this.timeout = timeout;
		this.node = node;
		this.txnLayer = txnLayer;
		this.unRtned = new HashMap<Integer, Map<Integer, TXNPacket>>();
		this.seqNums = new HashMap<Integer, Integer>();
	}
	
	/**
	 * @param dest
	 * @param seqNum
	 * @return true if the given seqNum has not yet been returned
	 */
	public boolean onRtn(int dest, int seqNum){
		return this.unRtned.get(dest) != null && this.unRtned.get(dest).remove(seqNum) != null;
	}
	
	public void onTimeout(Integer dest, Integer seqNum){
		TXNPacket pkt = this.unRtned.get(dest).remove(seqNum);
		if(pkt != null)
			this.txnLayer.onRIOTimeout(dest, pkt.pack());
	}
	
	public int nextSeqNum(int dest){
		int seqNum;
		if(this.seqNums.containsKey(dest))
			seqNum = this.seqNums.get(dest);
		else
			seqNum = -1;
		
		seqNum++;
		this.seqNums.put(dest, seqNum);
		this.updateDisk();
		return seqNum;
	}
	
	private void updateDisk(){
		try {
			String contents = "";
			for(Integer dest : this.seqNums.keySet()){
				contents += dest + " " + this.seqNums.get(dest) + "\n";
			}
			this.node.write(".txnSeq", contents, false, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void createTimeoutListener(int dest, TXNPacket pkt){
		try{
			this.addPkt(dest, pkt);
			Method onTimeoutMethod = Callback.getMethod("onTimeout", this, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			this.node.addTimeout(new Callback(onTimeoutMethod, this, new Object[]{ dest, pkt.getSeqNum() }), this.timeout);
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
