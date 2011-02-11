import java.util.LinkedList;
import java.util.Queue;

public class File {
	public static final int INV = 0;
	public static final int RO = 1;
	public static final int RW = 2;
	
	private int state;
	private String name;
	private Queue<Queueable> queuedCommands;
	private int version;
	
	public File(int state, String name){
		if(!isValidState(state)){
			throw new IllegalArgumentException("Not acceptable state.");
		}
		this.state = state;
		this.name = name;
		this.queuedCommands = new LinkedList<Queueable>();
		this.version = 0;
	}
	
	public int getState(){
		return this.state;
	}
	
	public int getVersion(){
		return this.version;
	}
	
	public void setState(int state){
		if(!isValidState(state)){
			throw new IllegalArgumentException("Not acceptable state.");
		}
		this.state = state;
	}
	
	public String getName(){
		return this.name;
	}
	
	public Queueable execute(){
		return this.queuedCommands.poll();
	}
	
	public Queueable peek(){
		return this.queuedCommands.peek();
	}
	
	public boolean execute(Queueable c){
		this.queuedCommands.add(c);
		return this.queuedCommands.size() == 1;
	}
	
	public static boolean isValidState(int s){
		return s == INV || s == RO || s == RW;
	}
	
	public int hashCode(){
		return this.name.hashCode();
	}
	
	public String toString(){
		String q = "[";
		for(Queueable t : this.queuedCommands)
			q += ((Command)t).toString() + ", ";
		
		return "Name: " + this.name + " Permissions: " + this.state + " Queued Commands: " + (q.length() == 1 ? "" : q.substring(0, q.length() - 2)) + "]";
	}
}
