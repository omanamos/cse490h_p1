import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class Commit implements Iterable<Integer>{
	
	private Log log;
	
	private boolean abort;
	/**
	 * key = client that this commit is waiting on
	 */
	private Set<Integer> wait;
	
	private int seqNum;
	
	public Commit(int client, Log log, Set<Integer> assumedCrashed, int seqNum){
		this.seqNum = seqNum;
		this.log = log;
		this.abort = false;
		this.wait = new HashSet<Integer>();
		for(MasterFile f : log){
			Update u = log.getInitialVersion(f);
			if(f.getPermissions(client) == File.INV || f.getState() == File.INV || assumedCrashed.contains(u.source) || u.source == -1 && log.hasReads(f)){
				//Dep transaction aborted -> this one must also abort
				this.abort = true;
				return;
			}else if(u.source == -1){
				//Shouldn't ever happen
			}else if(u.source > 0){
				//this client didn't get its initial version from the server
				if(u.version > f.getVersion()){
					//if there are reads or writes that depend on an uncommitted and unaborted transaction
					this.wait.add(u.source);
				}else if(u.version == f.getVersion() && f.getLastCommitter() != u.source){
					//two nodes wrote the same version, and the one you don't depend on committed first
					//but the node you depend on hasn't aborted yet/attempted to commit
					this.abort = true;
				}else if(u.version < f.getVersion() && log.hasWrites(f)){
					//the version you wrote to is old
					this.abort = true;
				}
			}else if(u.source == TransactionLayer.MASTER_NODE && u.version < f.getVersion() && log.hasWrites(f)){
				//two nodes wrote same version from the master, but the other one committed first
				this.abort = true;
			}
		}
	}
	
	public boolean isDepOn(int addr){
		return this.wait.contains(addr);
	}
	
	public Log getLog(){
		return this.log;
	}
	
	public Iterator<Integer> iterator(){
		return this.wait.iterator();
	}
	
	public void remove(Integer node){
		this.wait.remove(node);
	}
	
	public boolean isWaiting(){
		return !this.wait.isEmpty();
	}
	
	public boolean abort(){
		return this.abort;
	}
	
	public int getSeqNum(){
		return this.seqNum;
	}
}
