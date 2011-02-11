import java.util.HashMap;
import java.util.Iterator;

//Master Files reside on the MASTER_NODE
public class MasterFile extends File implements Iterable<Integer>{
	public static final int FREE = 1;
	public static final int WF = 2;
	
	//address, state
	private boolean isWaiting;
	private HashMap<Integer, Integer> filePermissions;
	
	public MasterFile(String name) {
		super(File.INV, name);
		this.isWaiting = false;
		this.filePermissions = new HashMap<Integer,Integer>();
	}
	
	public boolean isWaiting(){
		return this.isWaiting;
	}
	
	public boolean hasCopy( int addr ) {
		return this.filePermissions.containsKey(addr);
	}

	public void changePermissions( int addr, int state ) {
		if(state == File.INV)
			this.filePermissions.remove(addr);
		else
			this.filePermissions.put(addr, state);
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
