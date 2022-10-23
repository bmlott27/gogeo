package utilities

import (
	"fmt"
)

// Function for handling errors
func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
