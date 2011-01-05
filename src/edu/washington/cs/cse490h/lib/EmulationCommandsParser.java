package edu.washington.cs.cse490h.lib;

/**
 * Parser for the Emulator. Basically just does away with addresses in commands.
 */
public class EmulationCommandsParser extends CommandsParser {
	@Override
	protected Event parseLine(String line) {
		line = line.trim();

		if (skipLine(line)) {
			return null;
		}

		String[] cmd = line.split("\\s+");

		if (cmd[0].equals("fail")) {
			return Event.getFailure(-1);
		}

		if (cmd[0].equals("start")) {
			return Event.getStart(-1);
		}

		return parseCommonCmds(cmd);
	}

	@Override
	protected Event parseNodeCmd(String[] cmd) {
		if (cmd.length < 1) {
			// Should've been caught earlier
			return null;
		}
		
		StringBuffer msg = new StringBuffer(cmd[0]);
		
		for(int i = 1; i < cmd.length; i++) {
			msg.append(" " + cmd[i]);
		}
		
		// Node addr does not matter for emulator
		return Event.getCommand(-1, msg.toString());
	}
}
