package io.dbmaster.dbstyle.api

import java.util.*

public final class MessageCollector {
    private final List<Message> messages = new ArrayList<Message>()

    /** @return the logged messages **/
    public List<Message> getMessages() {
        return messages;
    }

    /** Reset the object. **/
    public void reset() {
        messages.clear();
    }

    /**
     * Logs a message to be reported.
     * @param aMsg the message to log
     **/
    public void add(Message msg) {
        messages.add(msg);
    }

    /** @return the number of messages */
    public int size() {
        return messages.size();
    }
}
