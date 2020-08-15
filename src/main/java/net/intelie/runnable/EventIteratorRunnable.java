package net.intelie.runnable;

import net.intelie.challenges.EventIterator;

public class EventIteratorRunnable {
	
    private EventIterator eventIterator;
	
	public EventIteratorRunnable(EventIterator eventIterator) {
		this.eventIterator = eventIterator;
	}
	
	public Runnable moveNext(){

	    Runnable r = new Runnable(){
	        public void run(){
	        	eventIterator.moveNext();
	        }
	    };

	    return r;

	}
	
}
