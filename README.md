# golang_test

    
    During the task execution, I made the following assumptions:

    * In case of service blocking, the submitted set of messages will not be processed.
    * Attempting to process messages by the service in a blocked state does not increase the blocking time.
    * The service processes messages at the same speed, regardless of their quantity, and we do not need to find the optimal number of messages to process within the limits.
    * The `nextTry` period is calculated experimentally based on testing.
