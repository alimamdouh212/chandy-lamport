package main

func main() {
	sim := NewSimulator()
	readTopology("8nodes.top", sim)
	injectEvents("8nodes-concurrent-snapshots.events", sim)

}
