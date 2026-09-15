use std::ptr::NonNull;

/// Intrusive doubly linked list node used by cache policies.
#[derive(Debug)]
pub(crate) struct DoublyLinkedNode<T> {
    pub(crate) data: T,
    pub(crate) prev: Option<NonNull<Self>>,
    pub(crate) next: Option<NonNull<Self>>,
}

impl<T> DoublyLinkedNode<T> {
    pub(crate) fn new(data: T) -> Box<Self> {
        Box::new(Self {
            data,
            prev: None,
            next: None,
        })
    }
}

/// Intrusive doubly linked list utility shared across cache policies.
#[derive(Debug)]
pub(crate) struct DoublyLinkedList<T> {
    head: Option<NonNull<DoublyLinkedNode<T>>>,
    tail: Option<NonNull<DoublyLinkedNode<T>>>,
}

impl<T> Default for DoublyLinkedList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DoublyLinkedList<T> {
    pub(crate) fn new() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }

    pub(crate) fn head(&self) -> Option<NonNull<DoublyLinkedNode<T>>> {
        self.head
    }

    #[allow(dead_code)]
    pub(crate) fn tail(&self) -> Option<NonNull<DoublyLinkedNode<T>>> {
        self.tail
    }

    /// Inserts the node at the front (head) of the list.
    pub(crate) unsafe fn push_front(&mut self, mut node_ptr: NonNull<DoublyLinkedNode<T>>) {
        let node = unsafe { node_ptr.as_mut() };
        node.prev = None;
        node.next = self.head;

        if let Some(mut head) = self.head {
            unsafe { head.as_mut().prev = Some(node_ptr) };
        } else {
            self.tail = Some(node_ptr);
        }

        self.head = Some(node_ptr);
    }

    /// Inserts the node at the back (tail) of the list.
    pub(crate) unsafe fn push_back(&mut self, mut node_ptr: NonNull<DoublyLinkedNode<T>>) {
        let node = unsafe { node_ptr.as_mut() };
        node.next = None;
        node.prev = self.tail;

        if let Some(mut tail) = self.tail {
            unsafe { tail.as_mut().next = Some(node_ptr) };
        } else {
            self.head = Some(node_ptr);
        }

        self.tail = Some(node_ptr);
    }

    /// Moves an existing node to the front of the list.
    #[allow(dead_code)]
    pub(crate) unsafe fn move_to_front(&mut self, node_ptr: NonNull<DoublyLinkedNode<T>>) {
        unsafe {
            self.unlink(node_ptr);
            self.push_front(node_ptr);
        }
    }

    /// Unlinks `node_ptr` from the list without deallocating it.
    pub(crate) unsafe fn unlink(&mut self, mut node_ptr: NonNull<DoublyLinkedNode<T>>) {
        let node = unsafe { node_ptr.as_mut() };

        match node.prev {
            Some(mut prev) => unsafe { prev.as_mut().next = node.next },
            None => self.head = node.next,
        }

        match node.next {
            Some(mut next) => unsafe { next.as_mut().prev = node.prev },
            None => self.tail = node.prev,
        }

        node.prev = None;
        node.next = None;
    }

    /// Drops all nodes currently owned by the list.
    pub(crate) unsafe fn drop_all(&mut self) {
        let mut current = self.head;
        while let Some(node_ptr) = current {
            current = unsafe { node_ptr.as_ref().next };
            unsafe {
                drop(Box::from_raw(node_ptr.as_ptr()));
            }
        }
        self.head = None;
        self.tail = None;
    }
}

/// Drops the boxed node referenced by `ptr`.
pub(crate) unsafe fn drop_boxed_node<T>(ptr: NonNull<DoublyLinkedNode<T>>) {
    unsafe { drop(Box::from_raw(ptr.as_ptr())) }
}
