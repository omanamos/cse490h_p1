
public class File {
	public static final int INV = 0;
	public static final int RO = 1;
	public static final int RW = 2;
	
	private int state;
	private String name;
	
	public File(int state, String name){
		if(!isValidState(state)){
			throw new IllegalArgumentException("Not acceptable state.");
		}
		this.state = state;
		this.name = name;
	}
	
	public int getState(){
		return this.state;
	}
	
	public String getName(){
		return this.name;
	}
	
	public static boolean isValidState(int s){
		return s == INV || s == RO || s == RW;
	}
}
