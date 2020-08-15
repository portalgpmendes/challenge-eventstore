package net.intelie.impl;

import java.util.List;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

public class ConcurrentEventIterator implements EventIterator{
	
	private List<Event> events;
	private EventStore eventStore;
	private Event current;
	private int position;
	private boolean moveNext;
	
	public ConcurrentEventIterator(List<Event> events, EventStore eventStore) {
		this.events = events;
		this.eventStore = eventStore;
		position = 0;
		moveNext = false;
	}

	@Override
	public void close() throws Exception {
		
	}

	@Override
	public synchronized boolean moveNext() {
		if(position == events.size()) {
			moveNext = false;
			return moveNext;
		} else {
			current = events.get(position);
			position++;
			moveNext = true; 
			return moveNext;
		}
	
	}

	@Override
	public Event current() {
		if(moveNext)
		    return current;
		else
			throw new IllegalStateException();
	}

	@Override
	public synchronized void remove() {
		if(moveNext) {
			events.remove(current);
			eventStore.remove(current);
		}
		else
			throw new IllegalStateException();
	}

}
