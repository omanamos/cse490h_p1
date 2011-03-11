package facebook;

import transactions.Command;
import transactions.Transaction;

public class ReadPosts extends FacebookOperation{
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check that curUser is logged in
												"get [username]_wall",
												"txcommit"};
	public ReadPosts(User u){
		super(COMMANDS);
	}
	
	@Override
	public void onCommandFinish(Command c) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onAbort(Transaction txn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onCommit(Transaction txn) {
		// TODO Auto-generated method stub
		
	}

}
