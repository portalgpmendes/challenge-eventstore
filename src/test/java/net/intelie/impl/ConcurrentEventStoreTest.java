package net.intelie.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import net.intelie.challenges.Event;
import net.intelie.runnable.EventStoreRunnable;

import static org.junit.Assert.*;

public class ConcurrentEventStoreTest {
	
	private ConcurrentEventStore eventStore;
	private EventStoreRunnable eventStoreInsert;
	
	public void createEventsWithDistinctType() {	  
		Event event1 = new Event("Type 1", 1L);
		eventStore.insert(event1);
		
		Event event2 = new Event("Type 2", 2L);
		eventStore.insert(event2);
		
		Event event3 = new Event("Type 3", 3L);
		eventStore.insert(event3);		
	}
	
	public void createEventsWithSameType() {	  
		Event event1 = new Event("Type 1", 1L);
		eventStore.insert(event1);
		
		Event event2 = new Event("Type 1", 2L);
		eventStore.insert(event2);
		
		Event event3 = new Event("Type 1", 3L);
		eventStore.insert(event3);		
	}
	
	@Before
	public void setUp() {
		eventStore = new ConcurrentEventStore();
		eventStoreInsert = new EventStoreRunnable(eventStore);
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
		         .forEach(i -> service.submit(eventStoreInsert.insertEvent(i)));
		
		service.awaitTermination(1000, TimeUnit.MILLISECONDS);
		
		assertEquals(100, eventStore.totalEvents());
	}
	
	@Test
	public void testRemoveAll_EventWithInexistentType() {
		try{	
            eventStore.removeAll("AAA");        
            fail("Shoud have given error!");
        } catch (IllegalStateException e){
            assertEquals("Error! There are no events with type AAA", e.getMessage());
        }
	}
	
	@Test
	public void testRemoveAll_EventWithNullType() {
		try{		
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
		service.submit(eventStoreInsert.removeAll("Type 1"));
		
		service.awaitTermination(1000, TimeUnit.MILLISECONDS);
		
		assertEquals(0, eventStore.totalEvents());
	}
	

}
