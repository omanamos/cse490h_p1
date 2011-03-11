package facebook;

import java.util.LinkedList;
import java.util.Queue;

import transactions.Command;
import transactions.Transaction;

public abstract class FacebookOperation {
	protected Queue<String> cmds;
	
	public FacebookOperation(String[] commands){
		this.cmds = new LinkedList<String>();
		for(String s : commands)
			this.cmds.add(s);
	}
	
	public abstract void onCommandFinish(Command c);
	public abstract void onAbort(Transaction txn);
	public abstract void onCommit(Transaction txn);
}
