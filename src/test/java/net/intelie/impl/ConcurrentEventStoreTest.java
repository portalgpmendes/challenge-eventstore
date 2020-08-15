package net.intelie.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
	
	private void assertEvent(String type, long timestamp) {
		Event event = eventIterator.current();
		assertEquals(type, event.type());
		assertEquals(timestamp, event.timestamp());
	}
	
	private void assertEvents(String type, long initialTimeStamp) {
		long i = initialTimeStamp;
		while(eventIterator.moveNext()) {
			Event event = eventIterator.current();
			assertEvent(event.type(), i);
			i++;
		}
	}
	
	private void createEvents() {	  
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
	
	private void createEventsWithDistinctType() {	  
		event1 = new Event("Type 1", 1L);
		eventStore.insert(event1);
		
		event2 = new Event("Type 2", 2L);
		eventStore.insert(event2);
		
		event3 = new Event("Type 3", 3L);
		eventStore.insert(event3);		
	}	

	private void createEventsWithSameType() {	  
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
		assertEquals(1, eventStore.totalEvents("Type 1"));
		assertEquals(1, eventStore.totalEvents("Type 2"));
		assertEquals(1, eventStore.totalEvents("Type 3"));
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
		
		// event1 and event2
		eventIterator = eventStore.query("Type 1", 1L, 3L);
		assertEquals(2, eventIterator.totalEvents());
		assertEvents("Type 1", 1L);
		
		// event1
		eventIterator = eventStore.query("Type 1", 1L, 2L);
		assertEquals(1, eventIterator.totalEvents());
		eventIterator.moveNext();
		assertEvent("Type 1", 1L);
		
		// event3
		eventIterator = eventStore.query("Type 2", 2L, 4L);
		assertEquals(1, eventIterator.totalEvents());
		eventIterator.moveNext();
		assertEvent("Type 2", 3L);
		
		// event7 e event8
		eventIterator = eventStore.query("Type 3", 7L, 9L);
		assertEquals(2, eventIterator.totalEvents());
		assertEvents("Type 3", 7L);
				
		// event5, event6 e event7
		eventIterator = eventStore.query("Type 3", 5L, 8L);
		assertEquals(3, eventIterator.totalEvents());	
		assertEvents("Type 3", 5L);
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
	public void testRemove() throws InterruptedException {
		createEvents();
		
		assertEquals(8, eventStore.totalEvents());
		assertEquals(2, eventStore.totalEvents("Type 1"));
		assertEquals(5, eventStore.totalEvents("Type 3"));
		
		// Removes event1 with Type 1
		eventStore.remove(event1);
		assertEquals(7, eventStore.totalEvents());
		assertEquals(1, eventStore.totalEvents("Type 1"));
		
		// Removes event7 with Type 3
		eventStore.remove(event7);
		assertEquals(6, eventStore.totalEvents());
		assertEquals(4, eventStore.totalEvents("Type 3"));;
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
	public void testRemoveAll() {
		createEvents();
		
		assertEquals(2, eventStore.totalEvents("Type 1"));
		assertEquals(1, eventStore.totalEvents("Type 2"));
		assertEquals(5, eventStore.totalEvents("Type 3"));
		
		eventStore.removeAll("Type 3");
		assertEquals(0, eventStore.totalEvents("Type 3"));
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
