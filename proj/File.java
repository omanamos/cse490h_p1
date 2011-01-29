import java.util.LinkedList;
import java.util.Queue;

public class File {
	public static final int INV = 0;
	public static final int RO = 1;
	public static final int RW = 2;
	
	public static final int WFN = 0;
	public static final int WFR = 1;
	public static final int WFW = 2;
	
	private int state;
	private String name;
	private int waitState;
	private Queue<Command> queuedCommands;
	
	public File(int state, String name){
		if(!isValidState(state)){
			throw new IllegalArgumentException("Not acceptable state.");
		}
		this.state = state;
		this.name = name;
		this.waitState = WFN;
		this.queuedCommands = new LinkedList<Command>();
	}
	
	public int getState(){
		return this.state;
	}
	
	public String getName(){
		return this.name;
	}
	
	public Command execute(){
		if(this.queuedCommands.size() == 1)
			this.waitState = WFN;
		else
			if(this.queuedCommands.peek().getType() == Command.GET)
				this.waitState = WFR;
			else
				this.waitState = WFW;
		return this.queuedCommands.poll();
	}
	
	public boolean execute(Command c){
		if(this.waitState == WFN){
			if(c.getType() == Command.GET)
				this.waitState = WFR;
			else
				this.waitState = WFW;
			return true;
		}else{
			this.queuedCommands.add(c);
			return false;
		}
	}
	
	public static boolean isValidWaitState(int s){
		return s == WFR || s == WFW || s == WFN;
	}
	
	public static boolean isValidState(int s){
		return s == INV || s == RO || s == RW;
	}
}
