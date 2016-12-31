# Blaze
A fast and lightweight message queueing framework over Redis.

Enqueue items by head (left). Along with the destination queue (termed SOURCE), another surrogate queue (termed SINK) is used 
for reliability and guaranteed message delivery. See explanation.

We do a LPUSH to enqueue items, which is a push from head. So the 'first in' item will always be at tail.
While dequeuing, thus popping is from the tail by a RPOP. To achieve data safety on dequeue
operation, we would do an atomic RPOPLPUSH (POP from tail of a SOURCE queue and push to head of a SINK queue).

This is leveraging the [Redis reliable queue pattern](https://redis.io/commands/rpoplpush#pattern-reliable-queue).
   ```
	 
	         == == == == ==
   Right --> ||	 ||	 ||  || <-- Left
(tail)       == == == == ==      (head)
```	
API Usage:
TBD
