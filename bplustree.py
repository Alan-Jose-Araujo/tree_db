import bisect

class BPlusTreeNode:
    def __init__(self, order, is_leaf=False):
        self.order = order
        self.is_leaf = is_leaf
        self.keys = []
        self.children_or_values = []
        self.next_leaf = None

    def is_full(self):
        return len(self.keys) == self.order


class BPlusTree:
    def __init__(self, order=5):
        if order < 3:
            raise ValueError("A ordem de uma Árvore B+ deve ser no mínimo 3.")
        self.root = BPlusTreeNode(order, is_leaf=True)
        self.order = order

    def _find_leaf(self, key):
        node = self.root
        while not node.is_leaf:
            i = bisect.bisect_right(node.keys, key)
            node = node.children_or_values[i]
        return node

    def _split_leaf(self, leaf, parent):
        mid_index = self.order // 2
        new_leaf = BPlusTreeNode(self.order, is_leaf=True)
        new_leaf.keys = leaf.keys[mid_index:]
        new_leaf.children_or_values = leaf.children_or_values[mid_index:]
        leaf.keys = leaf.keys[:mid_index]
        leaf.children_or_values = leaf.children_or_values[:mid_index]
        new_leaf.next_leaf = leaf.next_leaf
        leaf.next_leaf = new_leaf
        self._insert_in_parent(parent, new_leaf.keys[0], leaf, new_leaf)

    def _insert_in_parent(self, parent, key, left_child, right_child):
        if parent is None:
            new_root = BPlusTreeNode(self.order, is_leaf=False)
            new_root.keys = [key]
            new_root.children_or_values = [left_child, right_child]
            self.root = new_root
        else:
            idx = bisect.bisect_right(parent.keys, key)
            parent.keys.insert(idx, key)
            parent.children_or_values.insert(idx + 1, right_child)
            if parent.is_full():
                pass  # A divisão de nós internos pode ser adicionada aqui.

    def insert(self, key, value):
        parent = None
        node = self.root
        while not node.is_leaf:
            parent = node
            i = bisect.bisect_right(node.keys, key)
            node = node.children_or_values[i]

        try:
            idx = node.keys.index(key)
            node.children_or_values[idx] = value
            return
        except ValueError:
            pass

        idx = bisect.bisect_left(node.keys, key)
        node.keys.insert(idx, key)
        node.children_or_values.insert(idx, value)

        if node.is_full():
            self._split_leaf(node, parent)

    def search(self, key):
        leaf = self._find_leaf(key)
        try:
            idx = leaf.keys.index(key)
            return leaf.children_or_values[idx]
        except ValueError:
            return None

    def get_all(self):
        results = []
        node = self.root
        if not node.keys and not node.is_leaf:
            return []
        while not node.is_leaf:
            node = node.children_or_values[0]

        while node:
            results.extend(node.children_or_values)
            node = node.next_leaf
        return results
