package utilities

import (
	"fmt"
)

// Function for handling errors
func CheckErr(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
