/*
 * @Author: ZeroOneTaT
 * @Date: 2023-06-11 13:42:53
 * @LastEditTime: 2023-06-13 22:25:36
 * @FilePath: /MIT-6.824/src/main/mrcoordinator.go
 * @Description:
 *
 * Copyright (c) 2023 by ZeroOneTaT, All Rights Reserved.
 */
package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.824/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	// m := mr.MakeCoordinator(os.Args[1:], 2)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
