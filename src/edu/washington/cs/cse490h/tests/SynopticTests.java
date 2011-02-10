package edu.washington.cs.cse490h.tests;

import java.io.StringWriter;

import org.junit.Test;

import edu.washington.cs.cse490h.lib.SynopticLogger;
import static org.junit.Assert.*;

public class SynopticTests {
	/**
	 * Start a simulator with one node that attempts to trigger all
	 * the possible events, and check that we actually log something
	 * in repsonse to every event.
	 */
	@Test
	public void genAllEventsTest() {
		fail("TODO");
	}

	/**
	 * Create a synopticLogger, and use it to write out an event.
	 */
	@Test
	public void synopticLoggerTest() {
		StringWriter writer = new StringWriter();
		SynopticLogger synLogger = new SynopticLogger();

		synLogger.start(writer);
		synLogger.logEvent("time", "event");
		synLogger.stop();

		String generatedStr = writer.getBuffer().toString();
		String expectedStr = "time event\n";
		assertEquals(generatedStr, expectedStr);
	}

	/**
	 * Create a synopticLogger, and abuse it.
	 */
	@Test
	public void synopticLoggerExceptionsTest() {
		SynopticLogger synLogger = new SynopticLogger();
		// Try stopping it, without having started it.
		synLogger.stop();

		// Try logging with it, without having started it.
		synLogger.logEvent("time0", "event0");

		// Start it with filename that can't be opened.
		synLogger.start("\\/some\random/file");

		// Now try logging with it, even though the start failed.
		synLogger.logEvent("time1", "event1");

		// Now start it for real.
		StringWriter writer = new StringWriter();
		synLogger.start(writer);

		// And make sure that the log is empty.
		String generatedStr = writer.getBuffer().toString();
		String expectedStr = "";
		assertEquals(generatedStr, expectedStr);
	}
}
