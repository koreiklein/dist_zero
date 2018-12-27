struct queue
{
  // Initialize with count = 0 and data an array with enough space to hold the maximum number of elements.
	unsigned int count; // Count of the elements in the queue
	int *data; // Array with the elements
};

#define CMP(a, b) ((a) <= (b))

// Insert a value into the queue
void queue_push(struct queue *restrict h, int value)
{
	unsigned int index, parent;

	// Find out where to put the element and put it
	for(index = h->count++; index; index = parent)
	{
		parent = (index - 1) >> 1;
		if CMP(h->data[parent], value) break;
		h->data[index] = h->data[parent];
	}
	h->data[index] = value;
}

// Removes and returns the least element from the queue
int queue_pop(struct queue *restrict h)
{
  int result = h->data[0];
	unsigned int index, swap, other;

	// Remove the biggest element
	int temp = h->data[--h->count];

	// Reorder the elements
	for(index = 0; 1; index = swap)
	{
		// Find the child to swap with
		swap = (index << 1) + 1;
		if (swap >= h->count) break; // If there are no children, the queue is reordered
		other = swap + 1;
		if ((other < h->count) && CMP(h->data[other], h->data[swap])) swap = other;
		if CMP(temp, h->data[swap]) break; // If the lesser child is greater than or equal to its parent, the queue is reordered

		h->data[index] = h->data[swap];
	}
	h->data[index] = temp;
  return result;
}

