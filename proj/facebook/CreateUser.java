package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class CreateUser extends FacebookOperation{
	private static final String[] COMMANDS = {	"txstart",
												"get users", //check if user exists
												"append users \"[username] [password]\n\"",
												"create [username]_requests",
												"create [username]_friends",
												"create [username]_wall",
												"txcommit"};
	public CreateUser(User u, DistNode n){
		super(COMMANDS, n, u);
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
