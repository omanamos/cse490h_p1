import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

//Master Files reside on the MASTER_NODE
public class MasterFile extends File implements Iterable<Integer>{
	public static final int FREE = 1;
	public static final int WF = 2;
	
	private int lastCommitter;
	private boolean isWaiting;
	private HashMap<Integer, Integer> filePermissions;
	/**
	 * Requester depends on sender
	 * key = requester
	 * value = file state
	 */
	private PriorityQueue<Update> proposals;
	
	public MasterFile(String name, String contents) {
		super(File.INV, name);
		this.lastCommitter = -1;
		this.isWaiting = false;
		this.filePermissions = new HashMap<Integer, Integer>();
		this.proposals = new PriorityQueue<Update>();
	}
	
	public void commit(int client){
		this.lastCommitter = client;
		this.filePermissions.remove(client);
	}
	
	public void abort(int client){
		this.filePermissions.remove(client);
	}
	
	public int getLastCommitter(){
		return this.lastCommitter;
	}
	
	public Update chooseProp(int requestor){
		Update u = this.proposals.poll();
		this.proposals.clear();
		return u;
	}
	
	public void propose(String contents, int version, int source){
		this.proposals.add(new Update(contents, version, source));
	}
	
	public boolean isWaiting(){
		return this.isWaiting;
	}
	
	public boolean isCheckedOut(){
		return this.filePermissions.size() != 0;
	}
	
	public boolean hasCopy( int addr ) {
		return this.filePermissions.containsKey(addr);
	}
	
	public void changePermissions( int addr, int state ) {
		if(state == File.INV)
			this.filePermissions.remove(addr);
		else
			this.filePermissions.put(addr, state);
		if(state != MasterFile.WF){
			for(Integer s : this.filePermissions.values()){
				if(s == MasterFile.WF){
					this.isWaiting = true;
					return;
				}
			}
			this.isWaiting = false;
		}else{
			this.isWaiting = true;
		}
	}
	
	public int getPermissions(int addr){
		if(!this.filePermissions.containsKey(addr)){
			return File.INV;
		}else{
			return this.filePermissions.get(addr);
		}
	}
	
	public Iterator<Integer> iterator(){
		return this.filePermissions.keySet().iterator();
	}
}
