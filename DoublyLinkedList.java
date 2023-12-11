public class DoublyLinkedList {
    public Node head;

    public DoublyLinkedList() {
        this.head = null;
    }

    public void addNode(String joinAttribute) {
        Node newNode = new Node(joinAttribute);
        if (head == null) {
            head = newNode;
        } else {
            newNode.next = head;
            head.prev = newNode;
            head = newNode;
        }
    }

    public void deleteNode(String joinAttribute) {
        Node current = head;
        while (current != null) {
            if (current.joinAttribute.equals(joinAttribute)) {
                if (current.prev != null) {
                    current.prev.next = current.next;
                } else {
                    head = current.next;
                }
                if (current.next != null) {
                    current.next.prev = current.prev;
                }
                return;
            }
            current = current.next;
        }
    }

    static class Node {
        String joinAttribute;
        Node prev;
        Node next;

        public Node(String joinAttribute) {
            this.joinAttribute = joinAttribute;
            this.prev = null;
            this.next = null;
        }
    }
}
