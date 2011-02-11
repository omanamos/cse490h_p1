import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Utility;


public class TransactionLayer {

	private final static int MASTER_NODE = 0;
	
	private RIONode n;
	private ReliableInOrderMsgLayer RIOLayer;
	private Map<String, File> cache;
	private int lastTXNnum;
	private Transaction curTXN;

	public TransactionLayer(RIONode n, ReliableInOrderMsgLayer RIOLayer){
		this.cache = new HashMap<String, File>();
		this.n = n;
		this.RIOLayer = RIOLayer;
		this.lastTXNnum = n.addr;
	}

	public void send(int server, int protocol, byte[] payload) {
		TXNPacket pkt = new TXNPacket(protocol, payload);
		this.RIOLayer.sendRIO(server, Protocol.RPC, pkt.pack());
	}
	
	public void onRPCReceive(int from, byte[] payload) {
		TXNPacket packet = TXNPacket.unpack(payload);
		if(this.n.addr == MASTER_NODE)
			masterReceive(from, packet);
		else
			slaveReceive(packet);
	}
	
	public void onTimeout(int from, byte[] payload){
		
	}
	
	private void masterReceive(int from, TXNPacket pkt){
		
	}
	
	//TODO: execute command queue somewhere in here
	private void slaveReceive(TXNPacket pkt){
		switch(pkt.getProtocol()){
		case TXNProtocol.WF:
			break;
		case TXNProtocol.WD:
			break;
		case TXNProtocol.ABORT:
			this.abort(true);
			break;
		}
	}


	
	
	
	
	
	/*=====================================================
	 * Methods DistNode uses to talk to TXNLayer
	 *=====================================================*/
	
	public boolean get(String filename){
		File f = this.cache.get( filename );
		Command c = new Command(MASTER_NODE, Command.GET, f);
		
		if(f.execute(c)){
			return get(c, f);
		}
		
		return false;
	}
	
	private boolean get(Command c, File f){
		if(f.getState() == File.INV){
			this.send(MASTER_NODE, RPCProtocol.GET, Utility.stringToByteArray(f.getName())			return false;
		}else{

			try {
				f.execute();
				this.n.printData(this.n.get(f.getName()));
			} catch (IOException e) {
				this.n.printError(c, Error.ERR_10);
			}
			return true;
		}	
	}

	public void create(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void put(String fileName, String content) {
		// TODO Auto-generated method stub
		
	}

	public void append(String fileName, String content) {
		// TODO Auto-generated method stub
		
	}

	public void delete(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void abort(boolean retry) {
		// TODO Auto-generated method stub
		
	}

	public void commit() {
		// TODO Auto-generated method stub
		
	}

	public void start() {
		int newTXNnum = this.lastTXNnum + RIONode.NUM_NODES;
		this.curTXN = new Transaction( newTXNnum );
	}
	
	
}
