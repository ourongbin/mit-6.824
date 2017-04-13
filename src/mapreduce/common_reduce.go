package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	key2values := make(map[string][]string)
	var kv KeyValue
	for m := 0; m < nMap; m++ {
		fileIn, e := os.Open(reduceName(jobName, m, reduceTaskNumber))
		if e != nil {
			log.Println("os.Open:", reduceName(jobName, m, reduceTaskNumber), e)
			return
		}
		dec := json.NewDecoder(fileIn)
		for {
			if e := dec.Decode(&kv); e != nil {
				break
			}
			key2values[kv.Key] = append(key2values[kv.Key], kv.Value)
		}
		fileIn.Close()
	}
	var keys []string
	for k := range key2values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var key2reduced []KeyValue
	for _, k := range keys {
		key2reduced = append(key2reduced, KeyValue{k, reduceF(k, key2values[k])})
	}

	fileOut, e := os.OpenFile(mergeName(jobName, reduceTaskNumber),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if e != nil {
		log.Println("os.OpenFile:", mergeName(jobName, reduceTaskNumber), e)
		return
	}
	enc := json.NewEncoder(fileOut)
	for i := 0; i < len(key2reduced); i++ {
		e := enc.Encode(key2reduced[i])
		if e != nil {
			log.Println("enc.Encode:", key2reduced[i], e)
			return
		}
	}

	fileOut.Close()
}
