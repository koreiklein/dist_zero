#include <stdint.h>
#include <stdlib.h>

struct event
{
  uint64_t when;
  void (*occur)(void *data);
  void *data;
};

struct event_queue
{
  // Initialize with count = 0 and data an array with enough space to hold the maximum number of elements.
	unsigned int count; // Count of the elements in the queue
  unsigned int cap; // Capacity of the queue
	struct event *data; // Array with the elements
};

#define EVENT_CMP(a, b) (((a).when) <= ((b).when))

// Initialize a new queue
// return 1 on failure and 0 on success.
int event_queue_init(struct event_queue *queue, unsigned int cap) {
  queue->count = 0;
  queue->cap = cap;
  queue->data = (struct event *)malloc(cap * sizeof(struct event));
  if (queue->data == NULL) return 1;
  return 0;
}

// Insert a value into the queue, growing it if necessary.
// return 1 on failure and 0 on success.
int event_queue_push(struct event_queue *restrict h, struct event value)
{
	unsigned int index, parent;

  if (h->count == h->cap) {
    h->cap *= 2;
    h->data = (struct event *)realloc(h->data, h->cap * sizeof(struct event));
    if (h->data == NULL) return 1;
  }

	// Find out where to put the element and put it
	for(index = h->count++; index; index = parent)
	{
		parent = (index - 1) >> 1;
		if EVENT_CMP(h->data[parent], value) break;
		h->data[index] = h->data[parent];
	}
	h->data[index] = value;

  return 0;
}

// Remove and return the least event
struct event event_queue_pop(struct event_queue *restrict h)
{
  struct event result = h->data[0];
	unsigned int index, swap, other;

	// Remove the biggest element
	struct event * temp = &h->data[--h->count];

	// Reorder the elements
	for(index = 0; 1; index = swap)
	{
		// Find the child to swap with
		swap = (index << 1) + 1;
		if (swap >= h->count) break; // If there are no children, the queue is reordered
		other = swap + 1;
		if ((other < h->count) && EVENT_CMP(h->data[other], h->data[swap])) swap = other;
		if EVENT_CMP(*temp, h->data[swap]) break; // If the lesser child is greater than or equal to its parent, the queue is reordered

		h->data[index] = h->data[swap];
	}
	h->data[index] = *temp;
  return result;
}

