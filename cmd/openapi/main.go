package main

import (
	"fmt"
	"io"
	"os"

	"gpu-telemetry-pipeline/internal/api"
)

var fatalExit = os.Exit

func main() {
	osExit(writeSpec(os.Stdout))
}

func osExit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fatalExit(1)
	}
}

func writeSpec(w io.Writer) error {
	_, err := w.Write(append(api.OpenAPIJSON(), '\n'))
	return err
}
