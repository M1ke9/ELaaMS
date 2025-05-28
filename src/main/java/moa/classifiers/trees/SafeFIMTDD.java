package moa.classifiers.trees;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SafeFIMTDD extends FIMTDD implements Serializable {
    private static final long serialVersionUID = 1L;

    public SafeFIMTDD() { super(); }

    private void writeObject(ObjectOutputStream out) throws IOException {
        // Save state before detaching
        Node originalRoot = this.treeRoot;

        // Detach parent references
        detachAllCircularReferences(this.treeRoot);

        // Write the object
        out.defaultWriteObject();

        // Restore state after writing
        restoreAllCircularReferences(this.treeRoot, null);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        // Read the object
        in.defaultReadObject();

        // Restore parent references
        restoreAllCircularReferences(this.treeRoot, null);
    }

    private void detachAllCircularReferences(Node n) {
        if (n == null) return;

        // Detach parent
        n.parent = null;

        // Handle alternate tree
        if (n.alternateTree != null) {
            detachAllCircularReferences(n.alternateTree);
        }

        // Handle split node children
        if (n instanceof SplitNode) {
            SplitNode splitNode = (SplitNode) n;
            for (Node child : splitNode.children) {
                if (child != null) {
                    detachAllCircularReferences(child);
                }
            }

            // Handle any other potential circular references in SplitNode
            // Add more detachment logic as needed
        }

        // Handle any other potential circular references
        // This will depend on the actual structure of your model classes
    }

    private void restoreAllCircularReferences(Node n, Node p) {
        if (n == null) return;

        // Restore parent
        n.parent = p;

        // Handle alternate tree
        if (n.alternateTree != null) {
            restoreAllCircularReferences(n.alternateTree, n);
        }

        // Handle split node children
        if (n instanceof SplitNode) {
            SplitNode splitNode = (SplitNode) n;
            for (Node child : splitNode.children) {
                if (child != null) {
                    restoreAllCircularReferences(child, n);
                }
            }

            // Handle any other potential circular references in SplitNode
            // Add more restoration logic as needed
        }

        // Handle any other potential circular references
        // This will depend on the actual structure of your model classes
    }
}