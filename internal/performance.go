package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	ch := make(chan int, 2)
	go produce(ch)
	go consume(ch)

	start := time.Now()
	fmt.Println("[-]:Start file processing...")
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(){
			defer wg.Done()
			processFile(i)
		}()
	}

	wg.Wait()

	duration := time.Since(start)
	fmt.Printf("Task took %v\n", duration)

	fmt.Println("[OK]:Finished file processing.")
	
}


func processFile(fileId int){
	fmt.Printf("Processing File : %d. \n" , fileId)
	time.Sleep(2 * time.Second)
	fmt.Printf("Procssed File : %d. \n" , fileId)
}

func produce(ch chan <- int) {
	ch <- 1
}

func consume(ch <-chan int) {
	fmt.Println(<-ch)
}