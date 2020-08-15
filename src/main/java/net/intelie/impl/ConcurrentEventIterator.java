package net.intelie.impl;

import java.util.List;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

public class ConcurrentEventIterator implements EventIterator{
	
	private List<Event> events;
	private Event current;
	private int position;
	private boolean moveNext;
	
	public ConcurrentEventIterator(List<Event> events) {
		this.events = events;
		position = 0;
		moveNext = false;
	}
	
	@Override
	public int totalEvents() {               
		return events.size();
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
			throw new IllegalStateException("moveNext was never called or its last result was false!");
	}

	@Override
	public synchronized void remove() {
		if(moveNext) {
			events.remove(current);
		}
		else
			throw new IllegalStateException("moveNext was never called or its last result was false!");
	}

}
