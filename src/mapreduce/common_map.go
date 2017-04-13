package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	fileIn, e := os.Open(inFile)
	if e != nil {
		log.Println("os.Open:", inFile, e)
		return
	}
	contents, e := ioutil.ReadAll(fileIn)
	fileIn.Close()
	if e != nil {
		log.Println("ioutil.ReadAll:", fileIn, e)
		return
	}
	keyValues := mapF(inFile, string(contents))

	filesOut := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for r := 0; r < nReduce; r++ {
		fileOut, e := os.OpenFile(reduceName(jobName, mapTaskNumber, r),
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if e != nil {
			log.Println("os.Open:", reduceName(jobName, mapTaskNumber, r), e)
			return
		}
		filesOut[r] = fileOut
		encoders[r] = json.NewEncoder(fileOut)
	}
	for i := 0; i < len(keyValues); i++ {
		r := ihash(keyValues[i].Key) % nReduce
		e := encoders[r].Encode(keyValues[i])
		if e != nil {
			log.Println("enc.Encode:", keyValues[i], e)
			return
		}
	}

	for r := 0; r < nReduce; r++ {
		filesOut[r].Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
