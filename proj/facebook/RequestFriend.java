package facebook;

import transactions.Command;
import transactions.Transaction;

public class RequestFriend extends FacebookOperation {
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check if logged in as given requester
												"get users", //check friend exists
												"append [friend]_requests \"[requester]\n\"",
												"txcommit"};
	public RequestFriend(User requester, User friend){
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
