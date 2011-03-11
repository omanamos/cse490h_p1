package facebook;

import transactions.Command;
import transactions.Transaction;

public class Login extends FacebookOperation{
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check if logged in already
												"get users", //check if user exits
												"put logged_in \"[contents]\"",
												"txcommit"};

	public Login(User u){
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
