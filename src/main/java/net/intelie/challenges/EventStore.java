package net.intelie.challenges;

/**
 * An abstraction of an event store.
 * <p>
 * Events may be stored in memory, data files, a database, on a remote
 * server, etc. For this challenge, you should implement an in-memory
 * event event store.
 */
public interface EventStore {
	
	/**
	 * Calculates the total number of events. 
	 * @return number of events
	 */
	public int totalEvents();
	
	
	/**
	 * Calculates the total number of events by type.
	 * @param type
	 * @return
	 */
	public int totalEvents(String type);
	
    /**
     * Stores an event
     *
     * @param event
     */
    void insert(Event event);


    /**
     * Removes all events of specific type.
     *
     * @param type
     */
    void removeAll(String type);
    
    /** 
     * Removes one event of the store.
     * @param event
     */
    void remove(Event event);

    /**
     * Retrieves an iterator for events based on their type and timestamp.
     *
     * @param type      The type we are querying for.
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     * @return An iterator where all its events have same type as
     * {@param type} and timestamp between {@param startTime}
     * (inclusive) and {@param endTime} (exclusive).
     */
    EventIterator query(String type, long startTime, long endTime);
}
