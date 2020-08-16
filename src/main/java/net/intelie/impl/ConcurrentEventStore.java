package net.intelie.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

public class ConcurrentEventStore implements EventStore {
	
	private ConcurrentHashMap<String, List<Event>> eventMap;

	public ConcurrentEventStore() {
		eventMap = new ConcurrentHashMap<>();
	}
	
	@Override
	public int totalEvents() {
		int total = 0;
		
		Collection<List<Event>> eventLists = eventMap.values();
		if(eventLists != null) {
			total = eventLists.stream()
				              .mapToInt(list -> list.size())
				              .sum();
		}
	               
		return total;
	}
	
	@Override
	public int totalEvents(String type) {
		int total = 0;
		
		List<Event> eventsWithType = eventMap.get(type);
		if(eventsWithType != null) {
			total = eventsWithType.size();
		}
	               
		return total;
	}
	
	@Override
	public synchronized void insert(Event event) {
		if(event == null) {
			throw new NullPointerException("Error! Cannot insert a null event.");
		}
		
		List<Event> events;		
		
		if(eventMap.containsKey(event.type())) {
			events = eventMap.get(event.type());
			events.add(event);
		} else {
			events = new ArrayList<>();
			events.add(event);
			eventMap.put(event.type(), events);
		}
	}

	@Override
	public synchronized void removeAll(String type) {
		if(type == null) {
			throw new NullPointerException("Error! Cannot remove events with null type.");
		}
		
		List<Event> events = eventMap.get(type);
		if(events == null) {
			throw new IllegalStateException("Error! There are no events with type " + type);
		}
		
		events.clear();
	}
	
	@Override
	public synchronized void remove(Event event) {
		if(event == null) {
			throw new NullPointerException("Error! Cannot remove a null event.");
		}
		
		if(event.type() == null) {
			throw new NullPointerException("Error! Cannot remove events with null type.");
		}
		
		List<Event> events = eventMap.get(event.type());
		if(events == null) {
			throw new IllegalStateException("Error! There are no events with type " + event.type());
		}
		
		events.remove(event);
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		if(type == null) {
			throw new NullPointerException("Error! Cannot search for events with null type.");
		}
		
		List<Event> eventsStore = eventMap.get(type);
		if(eventsStore == null) {
			throw new IllegalStateException("Error! There are no events with type " + type);
		}
		
		if(startTime > endTime){
			throw new IllegalStateException("Error! startTime must be less than endTime");
		}
		
		List<Event> eventsIterator = eventsStore.stream()
                       .filter(event -> event.timestamp() >= startTime && event.timestamp() < endTime)
                       .collect(Collectors.toList());
		
		return new ConcurrentEventIterator(eventsIterator, eventsStore);
	} 

}
