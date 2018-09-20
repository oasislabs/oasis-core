package main

/*

Data table generator.

We use os/exec to do golang scripting: we run the driver with desired simulation parameters to
extract the speedup that is achievable.  Primarily we are interested in getting trend data:
suppose we hold the "normal" simulation parameters like zipf alpha, number of potential
conflict address locations, etc fixed, but varied the adversary's DOS injection probability
and/or batch size (i.e., change the expected number of DOS transactions); how will the system
parallelization ratio (increased throughput versus serial execution) respond?

*/

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/oasislabs/ekiden/go/scheduler/alg/iterflag"
	"github.com/oasislabs/ekiden/go/scheduler/alg/simulator"
)

var averageNumSample int
var iterationOrder string
var printMarkdown bool

func init() {
	flag.IntVar(&averageNumSample, "average-samples", 1, "compute speedup by averaging over this many runs")
	flag.StringVar(&iterationOrder, "iteration-order", "", "comma-separated list specifying the iteration order")
	flag.BoolVar(&printMarkdown, "markdown", false, "print tables using Markdown table extension notation")
}

func printFields(w io.Writer, fields []string, colWidth int) {
	if printMarkdown {
		for col := 0; col < len(fields); col++ {
			_, _ = fmt.Fprintf(w, "| %s ", fields[col])
		}
		_, _ = fmt.Fprintf(w, "|\n")
		return
	}
	for col := 0; col < len(fields); col++ {
		_, _ = fmt.Fprintf(w, "|%*s", colWidth, fields[col])
	}
	_, _ = fmt.Fprintf(w, "|\n")
}

func printSeparators(w io.Writer, numCols, colWidth int, dashRune string) {
	if printMarkdown {
		for col := 0; col < numCols; col++ {
			fmt.Fprintf(w, "| -------: ")
		}
		fmt.Fprintf(w, "|\n")
		return
	}
	for col := 0; col < numCols; col++ {
		_, _ = fmt.Fprintf(w, "+")
		for dash := 0; dash < colWidth; dash++ {
			_, _ = fmt.Fprintf(w, dashRune)
		}
	}
	_, _ = fmt.Fprintf(w, "+\n")
}

func printLightSeparators(w io.Writer, numCols, colWidth int) {
	printSeparators(w, numCols, colWidth, "-")
}

func printHeavySeparators(w io.Writer, numCols, colWidth int) {
	printSeparators(w, numCols, colWidth, "=")
}

// nolint: gocyclo
func main() {
	flag.Parse()
	iterflag.Parse()

	bw := bufio.NewWriter(os.Stdout)
	defer func(bw *bufio.Writer) {
		if err := bw.Flush(); err != nil {
			panic(fmt.Sprintf("I/O error: %s", err.Error()))
		}
	}(bw)

	simulator.UpdateAndCheckConfigFlags()

	dcnf := &simulator.DistributionConfigFromFlags
	acnf := &simulator.AdversaryConfigFromFlags
	lcnf := &simulator.LogicalShardingConfigFromFlags
	scnf := &simulator.SchedulerConfigFromFlags
	xcnf := &simulator.ExecutionConfigFromFlags

	// Check for I/O errors _now_ instead of running the whole simulation and
	// catching it in the deferred function, since the simulation is relatively
	// expensive and we should abort early.
	if bw.Flush() != nil {
		panic("I/O error")
	}

	var iterator *iterflag.Iterator
	var err error
	if iterationOrder != "" {
		allFlags := iterflag.AllIterableFlags()
		flagOrder := strings.Split(iterationOrder, ",")
		// Ensure that all specified flags are in allFlags.
		knownFlags := make(map[string]struct{})
		for _, f := range allFlags {
			knownFlags[f] = struct{}{}
		}
		for ix, f := range flagOrder {
			f = strings.Trim(f, " ") // remove spaces before/after comma
			flagOrder[ix] = f
			if _, found := knownFlags[f]; !found {
				panic(fmt.Sprintf("iteration-order specifies unknown flag %s", f))
			}
		}
		// Add unspecified flags to the end of flagOrder
		specifiedFlags := make(map[string]struct{})
		for _, f := range flagOrder {
			specifiedFlags[f] = struct{}{}
		}
		for _, f := range allFlags {
			if _, found := specifiedFlags[f]; !found {
				flagOrder = append(flagOrder, f)
			}
		}
		iterator, err = iterflag.MakeIteratorForFlags(flagOrder)
	} else {
		iterator, err = iterflag.MakeIterator()
	}
	if err != nil {
		panic(fmt.Sprintf("Iterator parsing error: %s", err.Error()))
	}

	// Print out all simulation parameters
	if simulator.Verbosity > 0 {
		if printMarkdown {
			fmt.Fprintf(bw, "```\n")
		}
		simulator.ShowConfigFlags(bw, *dcnf, *acnf, *lcnf, *scnf, *xcnf)
		fmt.Fprintf(bw, "\naverage-samples = %d\n", averageNumSample)
		fmt.Fprintf(bw, "iteration-order = \"%s\"\n\n", iterationOrder)
		if printMarkdown {
			fmt.Fprintf(bw, "```\n\n")
		}
		if bw.Flush() != nil {
			panic("I/O error for simulation configuration output")
		}
	}

	numVarying := 0
	vHeaders := make([]string, 0)
	for _, c := range iterator.Control {
		if c.WillIterate() {
			numVarying++
			vHeaders = append(vHeaders, c.Key())
		}
	}
	vHeaders = append(vHeaders, "Speedup")

	colWidth := 16
	precision := 4

	numCols := numVarying + 1
	if !printMarkdown {
		printHeavySeparators(bw, numCols, colWidth)
	}
	printFields(bw, vHeaders, colWidth)
	if printMarkdown {
		printLightSeparators(bw, numCols, colWidth)
	}

	for {
		if !printMarkdown {
			if iterator.AtStart(2) {
				printHeavySeparators(bw, numCols, colWidth)
			} else if iterator.AtStart(1) {
				printLightSeparators(bw, numCols, colWidth)
			}
		}
		data := make([]string, numCols)
		ix := 0
		for _, c := range iterator.Control {
			if c.WillIterate() {
				data[ix] = c.Value(colWidth, precision)
				ix++
			}
		}

		speedupSum := 0.0
		var res simulator.SimulationResults
		for sample := 0; sample < averageNumSample; sample++ {
			res = simulator.RunSimulationWithConfigs(dcnf, acnf, lcnf, scnf, xcnf, bw)
			speedup := float64(res.LinearExecutionTime) / float64(res.ActualExecutionTime)
			speedupSum += speedup
		}
		avgSpeedup := speedupSum / float64(averageNumSample)
		if simulator.Verbosity > 1 {
			_, _ = fmt.Fprintf(bw, "Linear execution time:    %8d\n", res.LinearExecutionTime)
			_, _ = fmt.Fprintf(bw, "Actual execution time:    %8d\n", res.ActualExecutionTime)
			_, _ = fmt.Fprintf(bw, "Speedup:                  %22.13f\n", avgSpeedup)
			_, _ = fmt.Fprintf(bw, "Number of schedules:      %8d\n", res.NumberOfSchedules)
		}
		data[numVarying] = fmt.Sprintf("%*.*g", colWidth, precision, avgSpeedup)

		printFields(bw, data, colWidth)
		if bw.Flush() != nil {
			panic("I/O error during summary statistics")
		}
		if !iterator.Incr() {
			break
		}
	}
	if !printMarkdown {
		printHeavySeparators(bw, numCols, colWidth)
	}
}
