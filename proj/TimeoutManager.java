import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Callback;


public class TimeoutManager {
	private TransactionLayer txnLayer;
	private DistNode node;
	private int timeout;
	private static final int MAX_RETRIES = 3;
	
	/**
	 * addr -> seqNum
	 */
	private Map<Integer, Integer> seqNums;
	/**
	 * addr -> seqNum -> packet
	 */
	private Map<Integer, Map<Integer, TXNPacket>> unRtned;
	/**
	 * addr -> seqNum -> number of retires
	 */
	private Map<Integer, Map<Integer, Integer>> retries;

	public TimeoutManager(int timeout, DistNode node, TransactionLayer txnLayer){
		this.timeout = timeout;
		this.node = node;
		this.txnLayer = txnLayer;
		this.unRtned = new HashMap<Integer, Map<Integer, TXNPacket>>();
		this.seqNums = new HashMap<Integer, Integer>();
		this.retries = new HashMap<Integer, Map<Integer, Integer>>();
	}
	
	public void initializeSeqNums(Map<Integer, Integer> seqNums){
		this.seqNums.putAll(seqNums);
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
		TXNPacket pkt = this.unRtned.get(dest).get(seqNum);
		Integer retries = this.retries.containsKey(dest) ? this.retries.get(dest).remove(seqNum) : null;
		if(pkt != null){
			if((retries == null || retries < MAX_RETRIES) && (pkt.getProtocol() == TXNProtocol.COMMIT || pkt.getProtocol() == TXNProtocol.ABORT)){
				if(!this.retries.containsKey(dest))
					this.retries.put(dest, new HashMap<Integer, Integer>());
				retries = retries == null ? 0 : retries;
				this.retries.get(dest).put(seqNum, retries + 1);
				
				this.createTimeoutListener(dest, pkt);
				this.txnLayer.RIOLayer.sendRIO(dest, Protocol.TXN, pkt.pack());
			}else{
				if(pkt.getProtocol() == TXNProtocol.COMMIT)
					System.out.println();
				this.txnLayer.onRIOTimeout(dest, pkt.pack());
			}
		}
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
	
	/**
	 * updates the seqNums stored on disk
	 */
	private void updateDisk(){
		try {
			String contents = "";
			for(Integer dest : this.seqNums.keySet()){
				contents += dest + " " + this.seqNums.get(dest) + "\n";
			}
			this.node.write(".txn_seq", contents, false, true);
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
