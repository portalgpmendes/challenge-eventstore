package net.intelie.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;
import net.intelie.runnable.EventStoreRunnable;

import static org.junit.Assert.*;

public class ConcurrentEventStoreTest {
	
	private EventStore eventStore;
	
	private EventIterator eventIterator;
	
	private EventStoreRunnable eventStoreRunnable;
	
	private Event event1;
	private Event event2;
	private Event event3;
	private Event event4;
	private Event event5;
	private Event event6;
	private Event event7;
	private Event event8;
	
	public void createEvents() {	  
		event1 = new Event("Type 1", 1L);
		eventStore.insert(event1);
		
		event2 = new Event("Type 1", 2L);
		eventStore.insert(event2);
		
		event3 = new Event("Type 2", 3L);
		eventStore.insert(event3);	
		
		event4 = new Event("Type 3", 4L);
		eventStore.insert(event4);	
		
		event5 = new Event("Type 3", 5L);
		eventStore.insert(event5);	
		
		event6 = new Event("Type 3", 6L);
		eventStore.insert(event6);
		
		event7 = new Event("Type 3", 7L);
		eventStore.insert(event7);
		
		event8 = new Event("Type 3", 8L);
		eventStore.insert(event8);
	}
	
	public void createEventsWithDistinctType() {	  
		event1 = new Event("Type 1", 1L);
		eventStore.insert(event1);
		
		event2 = new Event("Type 2", 2L);
		eventStore.insert(event2);
		
		event3 = new Event("Type 3", 3L);
		eventStore.insert(event3);		
	}
	
	public void createEventsWithSameType() {	  
		event1 = new Event("Type 1", 1L);
		eventStore.insert(event1);
		
		event2 = new Event("Type 1", 2L);
		eventStore.insert(event2);
		
		event3 = new Event("Type 1", 3L);
		eventStore.insert(event3);		
	}
	
	@Before
	public void setUp() {
		eventStore = new ConcurrentEventStore();
		eventStoreRunnable = new EventStoreRunnable(eventStore);
	}	
	
	@Test
	public void testInsert() {
		assertEquals(0, eventStore.totalEvents());
		createEventsWithDistinctType();
		assertEquals(3, eventStore.totalEvents());
	}
	
	@Test
	public void testInsert_NullEvent() {
		try{
            eventStore.insert(null);          
            fail("Shoud have given error!");
        } catch (NullPointerException e){
            assertEquals("Error! Cannot insert a null event.", e.getMessage());
        }
	}
	
	@Test
	public void testInsert_Syncronization() throws InterruptedException {
		assertEquals(0, eventStore.totalEvents());
		
		ExecutorService service = Executors.newFixedThreadPool(3);
		IntStream.range(0, 100)
		        .forEach(i -> service.submit(eventStoreRunnable.insertEvent(i)));

		service.awaitTermination(1000, TimeUnit.MILLISECONDS);
		
		assertEquals(100, eventStore.totalEvents());
	}
	
	@Test
	public void testQuery() {
		createEvents();
		
		eventIterator = eventStore.query("Type 1", 1L, 3L);
		assertEquals(2, eventIterator.totalEvents());
		
		eventIterator = eventStore.query("Type 1", 1L, 2L);
		assertEquals(1, eventIterator.totalEvents());
		
		eventIterator = eventStore.query("Type 3", 5L, 8L);
		assertEquals(3, eventIterator.totalEvents());
		
		long i = 5L;
		while(eventIterator.moveNext()) {
			Event event = eventIterator.current();
			assertEquals("Type 3", event.type());
			assertEquals(i, event.timestamp());
			i++;
		}
	}
	
	@Test
	public void testQuery_EventWithInexistentType() {
		try{	
			createEvents();
            eventStore.query("AAA", 1L, 3L);        
            fail("Shoud have given error!");
        } catch (IllegalStateException e){
            assertEquals("Error! There are no events with type AAA", e.getMessage());
        }
	}
	
	@Test
	public void testQuery_EventWithNullType() {
		try{	
			createEvents();
            eventStore.query(null, 1L, 3L);        
            fail("Shoud have given error!");
        } catch (NullPointerException e){
            assertEquals("Error! Cannot search for events with null type.", e.getMessage());
        }
	}
	
	@Test
	public void testQuery_StartTimeGreaterThanEndTime() {
		try{	
			createEvents();
            eventStore.query("Type 1", 3L, 1L);        
            fail("Shoud have given error!");
        } catch (IllegalStateException e){
            assertEquals("Error! startTime must be less than endTime", e.getMessage());
        }
	}
	
	@Test
	public void testRemove_EventWithInexistentType() {
		try{	
			createEventsWithDistinctType();
            eventStore.remove(new Event("AAA", 4L));        
            fail("Shoud have given error!");
        } catch (IllegalStateException e){
            assertEquals("Error! There are no events with type AAA", e.getMessage());
        }
	}
	
	@Test
	public void testRemove_EventWithNullType() {
		try{	
			createEventsWithDistinctType();
			eventStore.remove(new Event(null, 4L));      
            fail("Shoud have given error!");
        } catch (NullPointerException e){
            assertEquals("Error! Cannot remove events with null type.", e.getMessage());
        }
	}
	
	@Test
	public void testRemove_NullEvent() {
		try{	
			createEventsWithDistinctType();
			eventStore.remove(null);      
            fail("Shoud have given error!");
        } catch (NullPointerException e){
            assertEquals("Error! Cannot remove a null event.", e.getMessage());
        }
	}
	
	@Test
	public void testRemove_Syncronization() throws InterruptedException {
		assertEquals(0, eventStore.totalEvents());

		createEventsWithSameType();
		
		assertEquals(3, eventStore.totalEvents());
		
		ExecutorService service = Executors.newFixedThreadPool(3);
		service.submit(eventStoreRunnable.remove(event1));
		
		service.awaitTermination(1000, TimeUnit.MILLISECONDS);
		
		assertEquals(2, eventStore.totalEvents());
	}
	
	@Test
	public void testRemoveAll_EventWithInexistentType() {
		try{	
			createEventsWithDistinctType();
            eventStore.removeAll("AAA");        
            fail("Shoud have given error!");
        } catch (IllegalStateException e){
            assertEquals("Error! There are no events with type AAA", e.getMessage());
        }
	}
	
	@Test
	public void testRemoveAll_EventWithNullType() {
		try{	
			createEventsWithDistinctType();
            eventStore.removeAll(null);        
            fail("Shoud have given error!");
        } catch (NullPointerException e){
            assertEquals("Error! Cannot remove events with null type.", e.getMessage());
        }
	}
	
	@Test
	public void testRemoveAll_Syncronization() throws InterruptedException {
		assertEquals(0, eventStore.totalEvents());

		createEventsWithSameType();
		
		assertEquals(3, eventStore.totalEvents());
		
		ExecutorService service = Executors.newFixedThreadPool(3);
		service.submit(eventStoreRunnable.removeAll("Type 1"));
		
		service.awaitTermination(1000, TimeUnit.MILLISECONDS);
		
		assertEquals(0, eventStore.totalEvents());
	}
	

}