package gomq

import (
	"log"
	"runtime"
	"testing"
)

/*
 * Test End to End (MQ needs to be running and configured)
 */
func Test_MQ(test *testing.T) {
	mqconnection := MQ{}
	err := mqconnection.Connect()
	if err != nil {
		test.Error(err.Error())
	}

	messageQueue, err := mqconnection.Open("INPUT")
	if err != nil {
		test.Error(err.Error())
	}

	err = messageQueue.Put("TEST")
	if err != nil {
		test.Error(err.Error())
	}

	log.Println("1")
	runtime.GC()

	haveMessage, message, err := messageQueue.Get()
	if err != nil {
		test.Error(err.Error())
	}

	if haveMessage {
		log.Println(message)
	}

	log.Println("2")
	runtime.GC()

	haveMessage, message, err = messageQueue.Get()
	if err != nil {
		test.Error(err.Error())
	}

	if haveMessage {
		log.Println(message)
	}

	log.Println("3")
	runtime.GC()

	haveMessage, message, err = messageQueue.Get()
	if err != nil {
		test.Error(err.Error())
	}

	if haveMessage {
		log.Println(message)
	}

	err = messageQueue.Close()
	if err != nil {
		test.Error(err.Error())
	}

	err = mqconnection.Disconnect()
	if err != nil {
		test.Error(err.Error())
	}

}
