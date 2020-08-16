package net.intelie.impl;

import java.util.List;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

public class ConcurrentEventIterator implements EventIterator{
	
	private List<Event> eventsIterator;
	private List<Event> eventsStore;
	private Event current;
	private int position;
	private boolean moveNext;
	
	public ConcurrentEventIterator(List<Event> eventsIterator, List<Event> eventsStore) {
		this.eventsIterator = eventsIterator;
		this.eventsStore = eventsStore;
		position = 0;
		moveNext = false;
	}
	
	@Override
	public int totalEvents() {               
		return eventsIterator.size();
	}

	@Override
	public void close() throws Exception {
		
	}

	@Override
	public synchronized boolean moveNext() {
		if(position < eventsIterator.size()) {
			current = eventsIterator.get(position);
			position++;
			moveNext = true; 
			return moveNext;
		} else {
			moveNext = false;
			return moveNext;
		}
	}

	@Override
	public Event current() {
		if(!moveNext) {
		   throw new IllegalStateException("moveNext was never called or its last result was false!");
		}

		return current;
	}

	@Override
	public synchronized void remove() {
		if(!moveNext) {
		   throw new IllegalStateException("moveNext was never called or its last result was false!");
		}

		eventsIterator.remove(current);
		eventsStore.remove(current);
	}

}
