package ringtree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
)

var useArray = true      // Array or Red-Black tree
var branchFactor int = 1 // Global branch factor (can increase or decrease maxCount)
var NumReplicas int = 20 // Global number of replicas (vnodes)

// hash returns a hash value based on the key and level, ensuring remap compatibility.
func hash(key string, level int) uint32 {
	// Create a new Murmur3 hash instance.
	h := murmur3.New32()

	// Encode the level as binary data.
	levelBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(levelBytes, uint32(level))

	// Concatenate the key bytes with the level bytes and write them to the hash.
	h.Write([]byte(key))
	h.Write(levelBytes)

	// Return the computed hash value.
	return h.Sum32()
}

// Ring is the main structure for hierarchical consistent hashing implementation.
type Ring struct {
	id       string                 // Physical ring identifier
	level    int                    // Level of the hierarchy the ring exists on
	circle   Circle                 // Storing sorted virtual node hashes, maps virtual nodes to physical nodes
	members  map[string]interface{} // Tracks physical nodes and subrings objects on the ring
	maxCount int                    // Max nodes on the ring
	parent   *Ring                  // Reference to parent ring
	sync.RWMutex
}

// Node represents a node (physical server) in the ring tree.
type Node struct {
	id        string                        // Physical node identifer
	keys      map[uint32]map[string]*uint32 // Map of virtual nodes to key string to key hash
	load      int                           // Tracks load of node
	threshold int                           // Threshold of keys before node is considered overloaded
}

// newRingTree initializes a new ring tree at level 0.
func New(maxCount int) *Ring {
	remapped = 0
	numNodes = 0
	numKeys = 0
	if maxCount < 2 {
		maxCount = 2
	}
	r := newRing(nil, "main", 0, maxCount)
	return r
}

// newRing initializes a new subring with the current level's maxCount (adjusted by branchFactor).
func newRing(parent *Ring, id string, level int, maxCount int) *Ring {
	circle := NewCircle(useArray)
	return &Ring{
		id:       id,
		parent:   parent,
		level:    level,
		circle:   circle,
		members:  make(map[string]interface{}),
		maxCount: maxCount,
	}
}

// NewNode initialize a new Node with a threshold.
func NewNode(id string, threshold int) *Node {
	if id == "" {
		id = createId()
	}
	return &Node{
		id:        id,
		keys:      make(map[uint32]map[string]*uint32),
		load:      0,
		threshold: threshold,
	}
}

// InsertNode adds a physical node and its virtual nodes to the ring.
func (r *Ring) InsertNode(node *Node) error {
	defer timeTrack(time.Now(), "InsertNode", "to insert a node on level "+strconv.Itoa(r.level))
	r.Lock()
	defer r.Unlock()

	// Check if ring has reached the max number of physical nodes
	if len(r.members) >= r.maxCount {
		return errors.New("ring is at capacity")
	}
	if r.members[node.id] != nil {
		return errors.New("node is already in the ring")
	}

	// Add the node to members first
	r.members[node.id] = node

	// Add vNodes to the circle and remap keys after each addition
	for i := 0; i < NumReplicas; i++ {
		vNodeHash := hash(node.id, i)
		r.circle.Insert(vNodeHash, node.id)             // Insert vNode into the RB Tree
		node.keys[vNodeHash] = make(map[string]*uint32) // Initialize key map for this vNode
		fmt.Printf("Virtual node %d added to the ring.\n", vNodeHash)

		// Perform remapping for each vnode added
		if r.Size() > 1 {
			// Remap keys for this specific vnode
			err := r.remapKeys(node, vNodeHash)
			if err != nil {
				return err
			}
		}
	}

	fmt.Printf("Node %s successfully added to the ring.\n", node.id)
	numNodes++
	calculateRemapComplexity()
	return nil
}

// RemoveNode removes a physical node and its vNodes, from the ring and remaps its keys to the next closest node or subring.
func (r *Ring) RemoveNode(node *Node) error {
	// Check and collapse the ring if necessary
	if r.shouldCollapse() {
		_, err := r.collapseRing(node)
		return err
	}

	defer timeTrack(time.Now(), "RemoveNode", "to remove a node on level "+strconv.Itoa(r.level))
	r.Lock()
	defer r.Unlock()
	fmt.Printf("Removing node %s with load %d and remapping its keys.\n", node.id, node.load)

	// Iterate over the vNodes of the node being removed
	for vNodeHash := range node.keys {
		if node.load > 0 {
			// Find the next closest vNode in the ring for remapping
			nextVNodeHash, nextNodeId := r.circle.FindNextClosest(vNodeHash)
			for nextNodeId == node.id {
				nextVNodeHash, nextNodeId = r.circle.FindNextClosest(vNodeHash)
			}
			fmt.Printf("Remapping keys from vnode %d to next vnode %d (node %s).\n", vNodeHash, nextVNodeHash, nextNodeId)
			if nextNodeId == "" {
				return errors.New("no valid next node found for remapping")
			}
			// Handle the case where the next node is a subring
			switch nextNode := r.members[nextNodeId].(type) {
			case *Node:
				// Move the keys from the removed node's vNode to the next physical node's vNode
				for key, hashValue := range node.keys[vNodeHash] {
					r.moveKey(key, hashValue, node, vNodeHash, nextNode, nextVNodeHash)
				}
			case *Ring:
				// Remap the keys into the next subring
				fmt.Printf("Remapping keys into subring %s for vnode %d.\n", nextNode.id, nextVNodeHash)
				for key := range node.keys[vNodeHash] {
					remapped++
					numKeys--
					err := nextNode.InsertKey(key) // Insert the key into the subring
					if err != nil {
						fmt.Printf("Error inserting key %s into subring: %v\n", key, err)
						return err
					}
				}
			default:
				return errors.New("next node is not valid")
			}
		}

		// Remove keys from the old node's map
		delete(node.keys, vNodeHash)

		// Remove the vNode from the circle
		r.circle.Delete(vNodeHash)
		fmt.Printf("Virtual node %d removed from the ring.\n", vNodeHash)
	}

	if node.load != 0 {
		fmt.Printf("Node still has %d keys.\n", node.load)
		return errors.New("error removing keys from node")
	}

	// Remove the physical node from the members
	if _, exists := r.members[node.id]; exists {
		delete(r.members, node.id)
		//fmt.Printf("Node %s removed.\n", node.id)
	} else {
		return errors.New("node not found in members during removal")
	}

	numNodes--
	calculateRemapComplexity()
	return nil
}

// FindNode finds the node responsible for a given key.
func (r *Ring) FindNode(key string) (*Node, *Ring, uint32, *uint32, error) {
	/*r.RLock()
	defer r.RUnlock()*/

	if r.Size() == 0 {
		return nil, nil, 0, nil, errors.New("ring is empty")
	}

	// Hash the key and find the closest node in the ring
	keyHash := hash(key, r.level)
	vNodeHash, nodeId := r.circle.FindClosest(keyHash)
	fmt.Printf("FindNode found vNodeHash: %d, value: %s.\n", vNodeHash, nodeId)

	// Check if node id has a corresponding entry in the circle map
	if nodeId == "" || r.members[nodeId] == nil {
		return nil, nil, 0, nil, errors.New("hash not found in circle map")
	}

	// If the result is a subring, recurse into the subring
	switch node := r.members[nodeId].(type) {
	case *Node:
		return node, r, vNodeHash, &keyHash, nil
	case *Ring:
		return node.FindNode(key)
	default:
		return nil, nil, 0, nil, errors.New("invalid object in ring")
	}
}

// InsertKey inserts a key into the node that handles it. If the node is overloaded, the system balances the load.
func (r *Ring) InsertKey(key string) error {
	fmt.Printf("Inserting key %s.\n", key)
	node, parent, vNodeHash, keyHash, err := r.FindNode(key)
	fmt.Printf("FindNode for %d finished: %s.\n", *keyHash, node.id)
	if err != nil {
		return err
	}

	if node.keys[vNodeHash][key] != nil {
		return errors.New("key is already in ring")
	}

	// Add key if the node is not overloaded
	if node.load < node.threshold {
		node.keys[vNodeHash][key] = keyHash
		node.load++
		numKeys++
		fmt.Printf("Key %s inserted into node %s (Load: %d).\n", key, node.id, node.load)
	} else {
		// Node is overloaded, check if a new node can be added to the parent ring first
		if parent.Size() < parent.maxCount {
			fmt.Printf("Adding new node for key: %s\n", key)
			NewNode := NewNode("", node.threshold)
			err := parent.InsertNode(NewNode)
			if err != nil {
				return err
			}
			return parent.InsertKey(key)
		} else {
			// If the parent ring has reached its capacity, split the node into a subring
			fmt.Printf("Adding new subring for node: %s\n", node.id)
			subring, err := parent.splitNode(node)
			if err != nil {
				return errors.New("expected subring, got nil or invalid object")
			}
			fmt.Printf("Inserting key into subring: %s.\n", key)
			return subring.InsertKey(key)
		}
	}

	return nil
}

// RemoveKey removes a key from the ring (R0 or any subring).
func (r *Ring) RemoveKey(key string) error {
	fmt.Printf("Removing key %s.\n", key)

	// Find the node or subring responsible for the key
	node, parent, vNodeHash, _, err := r.FindNode(key)
	if err != nil {
		return err
	}

	r.Lock()
	// Check if the key exists in the vnode's keys map and remove it
	if _, exists := node.keys[vNodeHash]; exists {
		if _, keyExists := node.keys[vNodeHash][key]; keyExists {
			delete(node.keys[vNodeHash], key)

			node.load--
			fmt.Printf("Key %s removed from node %s (Load: %d).\n", key, node.id, node.load)

			// TODO: Handle underflow
			r.Unlock()
			if node.load <= int(float64(0.1)*float64(node.threshold)) {
				fmt.Printf("Before RemoveNode: ring size = %d\n", parent.Size())
				return parent.RemoveNode(node)
				//fmt.Printf("After RemoveNode: ring size = %d\n", parent.Size())
			}
			return nil
		}
	}

	r.Unlock()
	return errors.New("key not found in the ring")
}

// Lookup finds a key in the ring
func (r *Ring) Lookup(key string) (string, error) {
	defer timeTrack(time.Now(), "Lookup", "")
	fmt.Printf("Searching for key %s.\n", key)

	// Find the node or subring responsible for the key
	node, _, vNodeHash, _, err := r.FindNode(key)
	if err != nil {
		return "", err
	}

	// Check if the key exists in the vnode's keys map
	if _, exists := node.keys[vNodeHash]; exists {
		if _, keyExists := node.keys[vNodeHash][key]; keyExists {
			fmt.Printf("Found key %s at node %s.\n", key, node.id)
			return node.id, nil
		}
	}

	return "", errors.New("key not found")
}

// Members returns a list of all the members (servers) in the consistent hash circle.
func (r *Ring) Members() []string {
	r.RLock()
	defer r.RUnlock()

	var m []string
	for k := range r.members {
		m = append(m, k)
	}
	return m
}

// Size gets the number of physical nodes and rings.
func (r *Ring) Size() int {
	return len(r.members)
}

// splitNode converts an overloaded node into a subring.
func (r *Ring) splitNode(node *Node) (*Ring, error) {
	r.Lock()
	defer r.Unlock()
	numNodes--

	// Create a ring with the node's ID and replace the node with the ring in members
	// The virtual nodes in circle will now point to the subring
	subring := newRing(r, node.id, r.level+1, r.maxCount*branchFactor)
	r.members[node.id] = subring
	fmt.Printf("Created subring at level %d for node: %s\n", r.level+1, node.id)

	// Backup the old keys and id from the node
	oldKeys := node.keys
	oldNodeID := node.id

	// Add 2 nodes to the subring to balance the load
	err := subring.InsertNode(NewNode("", node.threshold))
	if err != nil {
		return nil, err
	}
	err = subring.InsertNode(NewNode("", node.threshold))
	if err != nil {
		return nil, err
	}

	// Re-insert the keys from the overloaded node into the subring
	for _, keysMap := range oldKeys {
		for key := range keysMap {
			//remapped++ // TODO: SOURCE
			numKeys--
			err := subring.InsertKey(key)
			if err != nil {
				return nil, fmt.Errorf("error reinserting key %s: %v", key, err)
			}
		}
	}

	fmt.Printf("Finished replacing node %s with subring\n", oldNodeID)
	calculateRemapComplexity()
	return subring, nil
}

// collapseRing merges the subring's nodes into a single node and reinserts all keys into the parent ring.
func (r *Ring) collapseRing(node *Node) (*Node, error) {
	defer timeTrack(time.Now(), "CollapseRing", "to collapse a ring on level "+strconv.Itoa(r.level))
	r.Lock()
	defer r.Unlock()

	// Ensure the subring has two or fewer members
	if len(r.members) > 2 {
		return nil, errors.New("can only collapse subrings with two or fewer nodes")
	}

	fmt.Printf("Collapsing ring %s.\n", r.id)

	// Ensure the parent ring exists
	if r.parent == nil {
		return nil, errors.New("cannot collapse root ring")
	}

	// Collect all keys from the current ring
	oldKeys := make(map[string]*uint32) // Flattened map of all keys in the subring
	for _, member := range r.members {
		if node, ok := member.(*Node); ok {
			// Gather all keys from each vnode
			for _, keys := range node.keys {
				for key, keyHash := range keys {
					oldKeys[key] = keyHash
				}
			}
		}
	}

	// Remove all members from the subring
	for _, member := range r.members {
		if node, ok := member.(*Node); ok {
			// Clear the node's keys and its membership
			node.keys = nil
			node.load = 0
		}
	}
	r.members = nil // Remove all subring members

	// Create a new node using the subring's ID and insert it into the parent ring
	newNode := NewNode(r.id, node.threshold)
	r.parent.members[newNode.id] = newNode

	// Add vNodes to the circle for the new node
	for i := 0; i < NumReplicas; i++ {
		vNodeHash := hash(newNode.id, i)
		newNode.keys[vNodeHash] = make(map[string]*uint32) // Initialize key map for this vNode
		fmt.Printf("Virtual node %d added to the parent ring.\n", vNodeHash)
	}

	// Reinsert all old keys into the parent ring
	for key, keyHash := range oldKeys {
		numKeys--
		if err := r.parent.InsertKey(key); err != nil {
			return nil, fmt.Errorf("error inserting key %s into parent ring: %v", key, err)
		}
		fmt.Printf("Reinserted key %s with hash %d into the parent ring.\n", key, *keyHash)
	}

	fmt.Printf("Collapsed subring %s into node %s and reinserted keys into parent ring\n", r.id, newNode.id)
	r.Unlock()
	r = nil
	return newNode, nil
}

// remapKeys remaps keys after each vnode has been added
func (r *Ring) remapKeys(newNode *Node, newVNodeHash uint32) error {
	fmt.Printf("Remapping keys for newly added vnode %d.\n", newVNodeHash)

	// Find the next vnode's hash and corresponding node ID in the ring
	nextVNodeHash, nextNodeId := r.circle.FindNextClosest(newVNodeHash)
	fmt.Printf("FindNextClosest found next vNodeHash: %d, value: %v.\n", nextVNodeHash, nextNodeId)

	// Handle the case where the next node is either a Node or a Ring
	switch nextNode := r.members[nextNodeId].(type) {
	case *Node:
		// Get the map of keys to hash values associated with the next vnode
		keyHashMap := nextNode.keys[nextVNodeHash]
		if len(keyHashMap) == 0 {
			fmt.Println("No keys found in the next vnode to remap.")
			return nil
		}

		fmt.Printf("%d keys found in the next vnode to check for remapping.\n", len(keyHashMap))

		// Iterate over the keys and check if they belong in the new vnode's hash range
		for key, hashValue := range keyHashMap {
			if r.shouldMove(hashValue, newVNodeHash, nextVNodeHash) {
				fmt.Printf("Key %s with hash %d is less than vnode %d, remapping from %d.\n", key, *hashValue, newVNodeHash, nextVNodeHash)
				r.moveKey(key, hashValue, nextNode, nextVNodeHash, newNode, newVNodeHash)
			}
		}

	case *Ring:
		// If the next node is a subring, we need to handle the keys within that subring
		nextNode.remapSubringKeys(r.level, newNode, newVNodeHash, nextVNodeHash)
		return nil
	default:
		return errors.New("next node is not valid for remapping")
	}
	return nil
}

// remaps keys within subrings
func (r *Ring) remapSubringKeys(level int, newNode *Node, newVNodeHash, nextVNodeHash uint32) error {
	// Iterate through the subring's members
	for _, member := range r.members {
		// Check if this is a deeper ring or a node
		switch node := member.(type) {
		case *Node:
			// Iterate over each vnode in the node's keys
			for vNodeHash, keyHashMap := range node.keys {
				// For each key in the vnode's key map
				for key := range keyHashMap {
					// Hash the key at the current level
					hashAtNewNodeLevel := hash(key, level)

					if r.shouldMove(&hashAtNewNodeLevel, newVNodeHash, nextVNodeHash) {
						fmt.Printf("Key %s with hash %d is less than vnode %d, remapping from %d.\n", key, hashAtNewNodeLevel, newVNodeHash, nextVNodeHash)
						r.moveKey(key, &hashAtNewNodeLevel, node, vNodeHash, newNode, newVNodeHash)
					}
				}
			}
		case *Ring:
			// Recursively go deeper into the subring, passing the same nextVNodeHash
			err := node.remapSubringKeys(level, newNode, newVNodeHash, nextVNodeHash)
			if err != nil {
				return err
			}
		default:
			return errors.New("invalid member found in subring")
		}
	}

	return nil
}

// moves a key from one node to another.
func (r *Ring) moveKey(key string, keyHash *uint32, oldNode *Node, oldVNodeHash uint32, newNode *Node, newVNodeHash uint32) {
	remapped++
	// Move the key from nextNode to NewNode
	delete(oldNode.keys[oldVNodeHash], key) // Remove from old vnode
	if newNode.keys[newVNodeHash] == nil {
		newNode.keys[newVNodeHash] = make(map[string]*uint32)
	}
	newNode.keys[newVNodeHash][key] = keyHash // Add to new vnode
	oldNode.load--                            // Decrement load of old node
	newNode.load++                            // Increment load of new node
	fmt.Printf("Key %s remapped from vnode %d to vnode %d\n", key, oldVNodeHash, newVNodeHash)
}

// Determines if a key should move.
func (r *Ring) shouldMove(keyHash *uint32, newVNodeHash uint32, nextVNodeHash uint32) bool {
	// Wraparound case: newVNodeHash is larger than nextVNodeHash
	if nextVNodeHash < newVNodeHash {
		// Move the key from the smallest hash if its less newVNodeHash or before nextVNodeHash (wraparound)
		if *keyHash <= newVNodeHash && *keyHash > nextVNodeHash {
			return true
		}
	} else {
		// Regular case
		if *keyHash <= newVNodeHash {
			return true
			// if new hash is smallest, then take all the keys greater
		} else if *keyHash > newVNodeHash && *keyHash > nextVNodeHash {
			return true
		}
	}

	return false
}

// Determines if a ring should collapse.
func (r *Ring) shouldCollapse() bool {
	// Collapse if there are 2 or fewer members and none is a subring
	if len(r.members) < 2 && r.parent != nil {
		for _, member := range r.members {
			if _, ok := member.(*Ring); ok {
				return false // Found a subring; do not collapse
			}
		}
		return true // No subring, ready to collapse
	}
	return false // More than 2 members or root; no collapse
}
