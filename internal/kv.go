package internal

type KVPair struct {
	Key   string
	Value Position
}

type KVPairSlice []KVPair

func (a KVPairSlice) Len() int           { return len(a) }
func (a KVPairSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a KVPairSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
