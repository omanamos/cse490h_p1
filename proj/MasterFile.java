import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Master Files reside on the MASTER_NODE
public class MasterFile extends File {

	//address, state
	HashMap<Integer, Integer> filePermissions;
	
	public MasterFile(String name) {
		super(File.RW, name);
		this.filePermissions = new HashMap<Integer,Integer>();
	}
	
	public boolean hasCopy( int addr ) {
		return this.filePermissions.containsKey(addr);
	}

	public void changePermissions( int addr, int state ) {
		this.filePermissions.put(addr, state);

	}
	
	public Map<Integer, List<Integer>> getUpdates(int addr){
		Map<Integer, List<Integer>> returnMap = new HashMap<Integer, List<Integer>>();
		
		for( Integer i : filePermissions.keySet() ) {
			switch( filePermissions.get(i) ) {
				case File.RO:
					if(!returnMap.containsKey(File.RO))
						returnMap.put(File.RO, new ArrayList<Integer>());
					returnMap.get(File.RO).add(i);
					break;
				case File.RW:
					returnMap.put(File.RW, new ArrayList<Integer>());
					returnMap.get(File.RW).add( i );
					return returnMap;
			}
		}
		return returnMap;
	}
	
	public int getPermissions(int addr){
		if(!this.filePermissions.containsKey(addr)){
			return -1;
		}else{
			return this.filePermissions.get(addr);
		}
	}
}
