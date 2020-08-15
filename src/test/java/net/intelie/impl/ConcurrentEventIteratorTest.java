package net.intelie.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.runnable.EventIteratorRunnable;

public class ConcurrentEventIteratorTest {
	
	private EventIterator eventIterator1;
	private EventIterator eventIterator2;
	private EventIteratorRunnable eventIteratorRunnable;
	
	private List<Event> createEvents(int n) {
    	List<Event> events = new ArrayList<Event>();
    	
    	for(long i=0; i < n; i++) {
    		Event event = new Event("Type " + i, i);
    		events.add(event);
    	}
    	
    	return events;
    }
	
    @Before
	public void setUp() {			
		eventIterator1 = new ConcurrentEventIterator(createEvents(3));
		eventIterator2 = new ConcurrentEventIterator(createEvents(100));
		eventIteratorRunnable = new EventIteratorRunnable(eventIterator2);
	}
	
	@Test
	public void testCurrent_MoveNextFalse() {
		try{
			eventIterator1.current();        
            fail("Shoud have given error!");
        } catch (IllegalStateException e){
            assertEquals("moveNext was never called or its last result was false!", e.getMessage());
        }
	}
	
	@Test
	public void testIteratorLoop() {
		long i = 0L;
		while(eventIterator1.moveNext()) {
			Event current = eventIterator1.current();
			assertEquals("Type " + i, current.type());
			assertEquals(i, current.timestamp());
			i++;
		}
	}
	
	@Test
	public void testMoveNext() {
		assertEquals(3, eventIterator1.totalEvents());
		
		// while the end is not reached, the method must to return true
		assertEquals(true, eventIterator1.moveNext());  
		assertEquals(true, eventIterator1.moveNext());
		assertEquals(true, eventIterator1.moveNext());
		
		// the last one must be false
		assertEquals(false, eventIterator1.moveNext());
	}
	
	@Test
	public void testMoveNext_Syncronization() throws InterruptedException {	
		ExecutorService service = Executors.newFixedThreadPool(3);
		IntStream.range(0, 100)
		        .forEach(i -> service.submit(eventIteratorRunnable.moveNext()));

		service.awaitTermination(1000, TimeUnit.MILLISECONDS);
		
		Event current = eventIterator2.current();
		assertEquals("Type " + 99, current.type());
		assertEquals(99, current.timestamp());
	}
	
	@Test
	public void testRemove() {
		assertEquals(3, eventIterator1.totalEvents());
		
		eventIterator1.moveNext();
		
		Event current;
		
		// get the first event
		current = eventIterator1.current();
		assertEquals("Type 0", current.type());
		assertEquals(0L, current.timestamp());
		
		// remove the second event
		eventIterator1.remove();
		assertEquals(2, eventIterator1.totalEvents());
	}
	
	@Test
	public void testRemove_MoveNextFalse() {
		try{
			eventIterator1.remove();        
            fail("Shoud have given error!");
        } catch (IllegalStateException e){
            assertEquals("moveNext was never called or its last result was false!", e.getMessage());
        }
	}

}
