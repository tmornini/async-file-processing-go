package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
)

const parallelism = 8

type input struct {
	Hash string
	Word string
}

func main() {
	theMap := &map[string]string{}

	mapperChannel, mapperWaitGroup := startMapper(theMap)

	hasherChannel, hashWaitGroup := startHashers(mapperChannel)

	readerWaitGroup := startReaders(hasherChannel)

	readerWaitGroup.Wait()
	close(hasherChannel)

	hashWaitGroup.Wait()
	close(mapperChannel)

	mapperWaitGroup.Wait()

	for hash, word := range *theMap {
		fmt.Println(hash, ":", word)
	}
}

func startMapper(theMap *map[string]string) (chan *input, *sync.WaitGroup) {
	mapperWaitGroup := &sync.WaitGroup{}

	mapperChannel := make(chan *input)

	mapperWaitGroup.Add(1)

	go mapper(theMap, mapperChannel, mapperWaitGroup)

	return mapperChannel, mapperWaitGroup
}

func mapper(theMap *map[string]string, mapperChannel chan *input, mapperWaitGroup *sync.WaitGroup) {
	for input := range mapperChannel {
		(*theMap)[input.Hash] = input.Word
	}

	mapperWaitGroup.Done()
}

func startHashers(mapperChannel chan *input) (chan string, *sync.WaitGroup) {
	hasherWaitGroup := &sync.WaitGroup{}

	hasherChannel := make(chan string)

	for i := 0; i < parallelism; i++ {
		hasherWaitGroup.Add(1)

		go hasher(mapperChannel, hasherChannel, hasherWaitGroup)
	}

	return hasherChannel, hasherWaitGroup
}

func hasher(mapperChannel chan *input, hasherChannel chan string, hasherWaitGroup *sync.WaitGroup) {
	for word := range hasherChannel {
		hash := sha256.Sum256([]byte(word))

		mapperChannel <- &input{
			Hash: fmt.Sprintf("%x", hash),
			Word: word,
		}
	}

	hasherWaitGroup.Done()
}

func startReaders(hasherChannel chan string) *sync.WaitGroup {
	readerWaitGroup := &sync.WaitGroup{}

	// this would iterate over the files to be processed
	for i := 0; i < parallelism; i++ {
		readerWaitGroup.Add(1)

		go reader(hasherChannel, readerWaitGroup, "/usr/share/dict/words")
	}

	return readerWaitGroup
}

func reader(hasherChannel chan string, readerWaitGroup *sync.WaitGroup, pathname string) {
	reader, err := os.Open(pathname)

	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		err := scanner.Err()

		if err != nil {
			panic(err)
		}

		hasherChannel <- scanner.Text()
	}

	readerWaitGroup.Done()
}
