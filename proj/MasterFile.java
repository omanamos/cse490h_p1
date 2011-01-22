import java.util.HashMap;


public class MasterFile extends File {

	HashMap<Integer, Integer> filePermissions;
	
	public MasterFile(int state, String name) {
		super(state, name);
		this.filePermissions = new HashMap<Integer,Integer>();
	}

	public int getPermissions(int addr){
		if(!this.filePermissions.containsKey(addr)){
			return -1;
		}else{
			return this.filePermissions.get(addr);
		}
	}
}
