import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Commit implements Iterable<Integer>{
	
	private boolean abort;
	/**
	 * key = client that this commit is waiting on
	 */
	private Map<Integer, Boolean> wait;
	
	public Commit(int client, Log log){
		this.abort = false;
		this.wait = new HashMap<Integer,Boolean>();
		for(MasterFile f : log){
			int dep = f.getDep(client);
			if(dep == -1 && log.hasReads(f)){
				//Dep transaction aborted -> this one must also abort
				this.abort = true;
				return;
			}else if(dep == -1){
				//Shouldn't ever happen
			}else if(dep > 0){
				//this client didn't get its initial version from the server
				if(log.getInitialVersion(f) > f.getVersion()){
					//if there are reads or writes that depend on an uncommitted and unaborted transaction
					this.wait.put(dep, true);
				}else if(log.getInitialVersion(f) == f.getVersion() && f.getLastCommitter() != dep){
					//two nodes wrote the same version, and the one you don't depend on committed first
					//but the node you depend on hasn't aborted yet/attempted to commit
					this.abort = true;
				}else if(log.getInitialVersion(f) < f.getVersion() && log.hasWrites(f)){
					//the version you wrote to is old
					this.abort = true;
				}
			}else if(dep == TransactionLayer.MASTER_NODE && log.getInitialVersion(f) < f.getVersion() && log.hasWrites(f)){
				//two nodes wrote same version from the master, but the other one committed first
				this.abort = true;
			}
		}
	}
	
	public Iterator<Integer> iterator(){
		return this.wait.keySet().iterator();
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
}
