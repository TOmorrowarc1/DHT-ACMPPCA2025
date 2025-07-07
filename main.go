package main

import (
	"flag"
	"math/rand"
	"os"
	"time"
)

var (
	help     bool
	testName string
)

func init() {
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&testName, "test", "", "which test(s) do you want to run: basic/advance/all")

	flag.Usage = usage
	flag.Parse()

	if help || (testName != "basic" && testName != "advance1" && testName != "advance2" && testName != "all" && testName != "consistent") {
		flag.Usage()
		os.Exit(0)
	}

	rand.Seed(time.Now().UnixNano())
}

func main() {
	yellow.Printf("Welcome to DHT-2024 Test Program!\n\n")

	var basicFailRate float64
	var forceQuitFailRate float64
	var QASFailRate float64
	var ConsisFailRate float64

	switch testName {
	case "all":
		fallthrough
	case "basic":
		yellow.Println("Basic Test Begins:")
		basicPanicked, basicFailedCnt, basicTotalCnt := basicTest()
		if basicPanicked {
			red.Printf("Basic Test Panicked.")
			os.Exit(0)
		}

		basicFailRate = float64(basicFailedCnt) / float64(basicTotalCnt)
		if basicFailRate > basicTestMaxFailRate {
			red.Printf("Basic test failed with fail rate %.4f\n\n", basicFailRate)
		} else {
			green.Printf("Basic test passed with fail rate %.4f\n\n", basicFailRate)
		}

		if testName == "basic" {
			break
		}
		time.Sleep(afterTestSleepTime)
		fallthrough
	case "advance1":
		yellow.Println("Advance Test Begins:")

		/* ------ Force Quit Test Begins ------ */
		forceQuitPanicked, forceQuitFailedCnt, forceQuitTotalCnt := forceQuitTest()
		if forceQuitPanicked {
			red.Printf("Force Quit Test Panicked.")
			os.Exit(0)
		}

		forceQuitFailRate = float64(forceQuitFailedCnt) / float64(forceQuitTotalCnt)
		if forceQuitFailRate > forceQuitMaxFailRate {
			red.Printf("Force quit test failed with fail rate %.4f\n\n", forceQuitFailRate)
		} else {
			green.Printf("Force quit test passed with fail rate %.4f\n\n", forceQuitFailRate)
		}
		/* ------ Force Quit Test Ends ------ */

		if testName == "advance1" {
			break
		}
		time.Sleep(afterTestSleepTime)
		fallthrough
	case "advance2":
		/* ------ Quit & Stabilize Test Begins ------ */
		QASPanicked, QASFailedCnt, QASTotalCnt := quitAndStabilizeTest()
		if QASPanicked {
			red.Printf("Quit & Stabilize Test Panicked.")
			os.Exit(0)
		}

		QASFailRate = float64(QASFailedCnt) / float64(QASTotalCnt)
		if QASFailRate > QASMaxFailRate {
			red.Printf("Quit & Stabilize test failed with fail rate %.4f\n\n", QASFailRate)
		} else {
			green.Printf("Quit & Stabilize test passed with fail rate %.4f\n\n", QASFailRate)
		}
		/* ------ Quit & Stabilize Test Ends ------ */

	case "consistent":
		ConsisPanicked, ConsisFailedCnt, ConsisTotalCnt := ConsistencyTest()
		if ConsisPanicked {
			red.Printf("Consistency Test Panicked.")
			os.Exit(0)
		}

		ConsisFailRate = float64(ConsisFailedCnt) / float64(ConsisTotalCnt)
		if ConsisFailRate > ConsisMaxFailRate {
			red.Printf("Consistency test failed with fail rate %.4f\n\n", ConsisFailRate)
		} else {
			green.Printf("Consistency test passed with fail rate %.4f\n\n", ConsisFailRate)
		}
	}

	cyan.Println("\nFinal print:")
	if basicFailRate > basicTestMaxFailRate {
		red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
	} else {
		green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
	}
	if forceQuitFailRate > forceQuitMaxFailRate {
		red.Printf("Force quit test failed with fail rate %.4f\n", forceQuitFailRate)
	} else {
		green.Printf("Force quit test passed with fail rate %.4f\n", forceQuitFailRate)
	}
	if QASFailRate > QASMaxFailRate {
		red.Printf("Quit & Stabilize test failed with fail rate %.4f\n", QASFailRate)
	} else {
		green.Printf("Quit & Stabilize test passed with fail rate %.4f\n", QASFailRate)
	}
	if ConsisFailRate > ConsisMaxFailRate {
		red.Printf("Consistenet test failed with fail rate %.4f\n", QASFailRate)
	} else {
		green.Printf("Consistenet test passed with fail rate %.4f\n", QASFailRate)
	}
}

func usage() {
	flag.PrintDefaults()
}
