package net.intelie.runnable;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventStore;

public class EventStoreRunnable {
	
	private EventStore eventStore;
	
	public EventStoreRunnable(EventStore eventStore) {
		this.eventStore = eventStore;
	}
	
	public Runnable insertEvent(long i){

	    Runnable r = new Runnable(){
	        public void run(){
	        	Event event = new Event("Type " + i, i);
	    		eventStore.insert(event);
	        }
	    };

	    return r;

	}
	
	public Runnable removeAll(String type){

	    Runnable r = new Runnable(){
	        public void run(){
	    		eventStore.removeAll(type);
	        }
	    };

	    return r;

	}
	
	public Runnable remove(Event event){

	    Runnable r = new Runnable(){
	        public void run(){
	    		eventStore.remove(event);
	        }
	    };

	    return r;

	}

}
