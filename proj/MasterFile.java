import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

//Master Files reside on the MASTER_NODE
public class MasterFile extends File implements Iterable<Integer>{
	public static final int FREE = 1;
	public static final int WF = 2;
	
	private int lastCommitter;
	private boolean isWaiting;
	public int requestor;
	private HashMap<Integer, Integer> filePermissions;
	/**
	 * Requester depends on sender
	 * key = requester
	 * value = sender
	 */
	private HashMap<Integer, Integer> dependencies;
	private List<Update> updates;
	private PriorityQueue<Update> proposals;
	
	public MasterFile(String name, String contents) {
		super(File.INV, name);
		this.lastCommitter = -1;
		this.isWaiting = false;
		this.filePermissions = new HashMap<Integer,Integer>();
		this.requestor = -1;
		this.updates = new ArrayList<Update>();
		this.proposals = new PriorityQueue<Update>();
	}
	
	public void commit(int from){
		this.lastCommitter = from;//TODO
	}
	
	public int getLastCommitter(){
		return this.lastCommitter;
	}
	
	public void chooseProp(int requestor){
		Update u = this.proposals.poll();
		this.dependencies.put(requestor, u.source);
		this.updates.add(u);
		this.proposals.clear();
	}
	
	public void addDep(int requestor, int sender){
		this.dependencies.put(requestor, sender);
	}
	
	public void propose(String contents, int version, int source){
		this.proposals.add(new Update(contents, version, source));
	}
	
	public String getContents(){
		return this.updates.get(this.updates.size() - 1).contents;
	}
	
	public int getVersion(){
		return this.updates.get(this.updates.size() - 1).version;
	}
	
	public List<Update> getUpdates(){
		return this.updates;
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
