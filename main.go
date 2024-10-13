package main

import (
	"fmt"

	ringtree "github.com/kagwave/ring-tree/ringtree"
)

// Simulation parameters
var numKeys = 100000 // Number of keys
var τ = 100          // Max keys per node before splitting
var d = 7            // Maximum number of nodes on R0

func main() {
	// Insert keys and compare distributions
	/*fmt.Println("\nInserting keys into Flat Ring...")
	flatRing := SimulateInsertionsFlat()

	fmt.Println("\n--- Flat Consistent Hashing Stats ---")
	ringtree.PrintHierarchyDetails(flatRing)
	ringtree.PrintSystemVariance(flatRing)
	ringtree.PrintRemapStats()
	ringtree.PrintOperationTimeStats()*/

	fmt.Println("\nInserting keys into RingTree...")
	hierachicalRing := SimulateInsertions(true)

	fmt.Println("\n--- RingTree Stats ---")
	//ringtree.PrintLoadDetails(hierachicalRing)
	ringtree.PrintHierarchyDetails(hierachicalRing)
	ringtree.PrintSystemVariance(hierachicalRing)
	ringtree.PrintRemapStats()
	ringtree.PrintOperationTimeStats()

}

// SimulateInsertionsFlat inserts keys into a flat consistent hashing ring
func SimulateInsertionsFlat() *ringtree.Ring {
	rt := ringtree.New(1439) // Initialize a flat ring with capacity numKeys
	//node := ringtree.NewNode("", τ) // Set a high threshold to prevent splitting
	//rt.InsertNode(node)

	for i := 0; i < d; i++ {
		node := ringtree.NewNode("", τ) // Keep the threshold large to prevent splitting
		rt.InsertNode(node)
	}

	for i := 0; i < numKeys; i++ {
		key, _ := ringtree.GenerateRandomString(20)
		err := rt.InsertKey(key)
		if err != nil {
			fmt.Printf("Error inserting key: %v\n", err)
			return nil
		}

		/*// Add new node after reaching the threshold without splitting into subrings
		if (i+1)%(τ) == 0 {
			node := ringtree.NewNode(numKeys) // Keep the threshold large to prevent splitting
			rt.InsertNode(node)
		}*/
	}
	return rt
}

// SimulateInsertions simulates the insertion of keys into a hierarchical RingTree structure
func SimulateInsertions(remove bool) *ringtree.Ring {
	rt := ringtree.New(d)           // Start with an empty RingTree
	node := ringtree.NewNode("", τ) // Set a reasonable threshold for splitting
	rt.InsertNode(node)

	var keys []string

	for i := 0; i < d; i++ {
		node := ringtree.NewNode("", τ) // Keep the threshold large to prevent splitting
		rt.InsertNode(node)
	}

	for i := 0; i < numKeys; i++ {
		key, _ := ringtree.GenerateRandomString(20)
		keys = append(keys, key)
		err := rt.InsertKey(key)
		if err != nil {
			fmt.Printf("Error inserting key: %v\n", err)
			return nil
		}
	}

	if remove {
		fmt.Printf("\n\nRemoving...\n\n")

		for i := 0; i < 500; i++ {
			err := rt.RemoveKey(keys[i])
			if err != nil {
				fmt.Printf("Error removing key: %v\n", err)
				return nil
			}
		}
	}

	return rt
}
