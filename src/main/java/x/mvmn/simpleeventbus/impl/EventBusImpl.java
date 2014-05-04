package x.mvmn.simpleeventbus.impl;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import x.mvmn.lang.ExceptionHandler;
import x.mvmn.simpleeventbus.AbstractEvent;
import x.mvmn.simpleeventbus.EventBus;
import x.mvmn.simpleeventbus.EventListener;

public class EventBusImpl implements EventBus {

	protected static class EventHandlersQueueEntry {

		private EventListener<? extends AbstractEvent> eventListener;
		private ExceptionHandler<Throwable> exceptionHandler;

		protected EventHandlersQueueEntry(EventListener<? extends AbstractEvent> eventListener, ExceptionHandler<Throwable> exceptionHandler) {
			super();
			this.eventListener = eventListener;
			this.exceptionHandler = exceptionHandler;
		}

		public EventListener<? extends AbstractEvent> getEventListener() {
			return eventListener;
		}

		public ExceptionHandler<Throwable> getExceptionHandler() {
			return exceptionHandler;
		}
	}

	protected HashMap<Class<? extends AbstractEvent>, ConcurrentLinkedQueue<EventHandlersQueueEntry>> listenersMap = new HashMap<Class<? extends AbstractEvent>, ConcurrentLinkedQueue<EventHandlersQueueEntry>>();
	protected ReentrantReadWriteLock listenersMapLock = new ReentrantReadWriteLock();

	protected ConcurrentHashMap<Class<? extends AbstractEvent>, ExceptionHandler<Throwable>> exceptionHandlersMap = new ConcurrentHashMap<Class<? extends AbstractEvent>, ExceptionHandler<Throwable>>();

	public EventListener<? extends AbstractEvent> registerEventListener(Class<? extends AbstractEvent> eventClass,
			EventListener<? extends AbstractEvent> listener) {
		return registerEventListener(eventClass, listener, null);
	}

	public EventListener<? extends AbstractEvent> registerEventListener(Class<? extends AbstractEvent> eventClass,
			EventListener<? extends AbstractEvent> listener, ExceptionHandler<Throwable> eventHandlingErrorHandler) {
		ConcurrentLinkedQueue<EventHandlersQueueEntry> listenersQueue = getOrCreateListenersQueue(eventClass);
		listenersQueue.add(new EventHandlersQueueEntry(listener, eventHandlingErrorHandler));

		return listener;
	}

	public AbstractEvent fireEventOfAutodetectedType(AbstractEvent event) {
		return fireEventOfAutodetectedType(event, null);
	}

	public AbstractEvent fireEventOfAutodetectedType(AbstractEvent event, ExceptionHandler<Throwable> eventHandlingErrorHandler) {
		return doFireEvent(event.getClass(), event, eventHandlingErrorHandler, true);
	}

	public AbstractEvent fireEventOfExactType(Class<? extends AbstractEvent> eventClass, AbstractEvent event) {
		return fireEventOfExactType(eventClass, event, null);
	}

	public AbstractEvent fireEventOfExactType(Class<? extends AbstractEvent> eventClass, AbstractEvent event,
			ExceptionHandler<Throwable> eventHandlingErrorHandler) {
		return doFireEvent(eventClass, event, eventHandlingErrorHandler, false);
	}

	public AbstractEvent fireEventOfThisOrParentType(Class<? extends AbstractEvent> eventClass, AbstractEvent event) {
		return fireEventOfThisOrParentType(eventClass, event, null);
	}

	public AbstractEvent fireEventOfThisOrParentType(Class<? extends AbstractEvent> eventClass, AbstractEvent event,
			ExceptionHandler<Throwable> eventHandlingErrorHandler) {
		return doFireEvent(eventClass, event, eventHandlingErrorHandler, true);
	}

	// //////////////////////////////////////////////////////////////////////////////

	protected ConcurrentLinkedQueue<EventHandlersQueueEntry> getOrCreateListenersQueue(Class<? extends AbstractEvent> eventClass) {
		ConcurrentLinkedQueue<EventHandlersQueueEntry> result;
		ReadLock readLock = listenersMapLock.readLock();
		try {
			readLock.lock();
			result = listenersMap.get(eventClass);
		} finally {
			readLock.unlock();
		}
		if (result == null) {
			WriteLock writeLock = listenersMapLock.writeLock();
			try {
				writeLock.lock();
				result = listenersMap.get(eventClass);
				if (result == null) {
					result = new ConcurrentLinkedQueue<EventHandlersQueueEntry>();
					listenersMap.put(eventClass, result);
				}
			} finally {
				writeLock.unlock();
			}
		}

		return result;
	}

	protected ConcurrentLinkedQueue<EventHandlersQueueEntry> findExistingListenersQueue(Class<? extends AbstractEvent> eventClass, boolean searchByParentClasses) {
		ConcurrentLinkedQueue<EventHandlersQueueEntry> result = null;

		ReadLock readLock = listenersMapLock.readLock();
		try {
			readLock.lock();

			result = listenersMap.get(eventClass);
			if (result == null && searchByParentClasses) {
				Class<?> parentClass = eventClass.getSuperclass();
				while (result == null && AbstractEvent.class.isAssignableFrom(parentClass)) {
					result = listenersMap.get(parentClass);
					parentClass = parentClass.getSuperclass();
				}
			}
		} finally {
			readLock.unlock();
		}

		return result;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected AbstractEvent doFireEvent(Class<? extends AbstractEvent> eventClass, AbstractEvent event, ExceptionHandler<Throwable> exceptionHandlerOverride,
			boolean useParentEventClasses) {
		ConcurrentLinkedQueue<EventHandlersQueueEntry> listenersQueue = findExistingListenersQueue(eventClass, useParentEventClasses);

		if (listenersQueue != null) {
			for (EventHandlersQueueEntry listenersQueueEntry : listenersQueue) {
				EventListener eventListener = listenersQueueEntry.getEventListener();
				ExceptionHandler<Throwable> exceptionHandler = listenersQueueEntry.getExceptionHandler();
				try {
					eventListener.handleEvent(event);
				} catch (Throwable exceptionOnHandlingEvent) {
					if (exceptionHandler != null) {
						try {
							exceptionHandler.handleException(exceptionOnHandlingEvent);
						} catch (Throwable exceptionOnHandlingException) {
						}
					}
				}
			}
		}

		return event;
	}

}
