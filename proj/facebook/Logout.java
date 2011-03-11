package facebook;

import transactions.Command;
import transactions.Transaction;

public class Logout extends FacebookOperation {
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check if logged in as given User
												"put logged_in [contents]",
												"txcommit"};
	public Logout(User u){
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
