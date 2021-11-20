//go:build ignore
// +build ignore

// Copyright 2015, Klaus Post, see LICENSE for details.
//
// Simple encoder example
//
// The encoder encodes a simgle file into a number of shards
// To reverse the process see "simpledecoder.go"
//
// To build an executable use:
//
// go build simple-decoder.go
//
// Simple Encoder/Decoder Shortcomings:
// * If the file size of the input isn't divisible by the number of data shards
//   the output will contain extra zeroes
//
// * If the shard numbers isn't the same for the decoder as in the
//   encoder, invalid output will be generated.
//
// * If values have changed in a shard, it cannot be reconstructed.
//
// * If two shards have been swapped, reconstruction will always fail.
//   You need to supply the shards in the same order as they were given to you.
//
// The solution for this is to save a metadata file containing:
//
// * File size.
// * The number of data/parity shards.
// * HASH of each shard.
// * Order of the shards.
//
// If you save these properties, you should abe able to detect file corruption
// in a shard and be able to reconstruct your data if you have the needed number of shards left.

package main

import (
	"bufio"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/DurantVivado/reedsolomon"
)

var dataShards = flag.Int("data", 4, "Number of shards to split the data into, must be below 257.")
var parShards = flag.Int("par", 2, "Number of parity shards")
var blockSize = flag.Int64("bs", 1024, "block size")
var outDir = flag.String("out", "", "Alternative output directory")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  simple-encoder [-flags] filename.ext\n\n")
		fmt.Fprintf(os.Stderr, "Valid flags:\n")
		flag.PrintDefaults()
	}
}

func genRandomArr(n int) []int {
	shuff := make([]int, n)
	for i := 0; i < n; i++ {
		shuff[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
	return shuff
}
func main() {
	// Parse command line parameters.
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: No input filename given\n")
		flag.Usage()
		os.Exit(1)
	}
	if (*dataShards + *parShards) > 256 {
		fmt.Fprintf(os.Stderr, "Error: sum of data and parity shards cannot exceed 256\n")
		os.Exit(1)
	}
	fname := args[0]
	startTime := time.Now()
	// Create encoding matrix.
	enc, err := reedsolomon.New(*dataShards, *parShards)
	checkErr(err)

	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fs, err := os.Stat(fname)
	if err != nil {
		panic(err)
	}
	fileSize := fs.Size()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		panic(err)
	}
	hashStr := fmt.Sprintf("%x", h.Sum(nil))
	f.Seek(0, 0)
	fmt.Println("Opening", fname, ",hash", hashStr)
	stripeno := int64(0)
	stripeSize := int64(*dataShards) * (*blockSize)
	data := make([]byte, stripeSize)
	of := make([]*os.File, *dataShards+*parShards)
	buf := bufio.NewReader(f)
	stripeNum := (fileSize + stripeSize - 1) / stripeSize
	distribution := make([][]int, stripeNum)
	for {

		b, err := buf.Read(data)
		if err != nil && err != io.EOF {
			panic(err)
		}

		// Split the file into equally sized shards.
		shards, err := enc.Split(data)
		checkErr(err)
		fmt.Printf("stripe:%d, File split into %d data+parity shards with %d bytes/shard.\n", stripeno, len(shards), len(shards[0]))
		// Encode parity
		err = enc.Encode(shards)
		checkErr(err)

		// Write out the resulting files.
		dir, file := filepath.Split(fname)
		if *outDir != "" {
			dir = *outDir
		}
		distribution[stripeno] = genRandomArr(*dataShards + *parShards)
		for i := range shards {
			j := distribution[stripeno][i]
			outfn := fmt.Sprintf("%s.%d", file, i)

			// fmt.Println("Writing to", outfn)
			of[i], err = os.OpenFile(filepath.Join(dir, outfn), os.O_CREATE|os.O_APPEND, 0644)
			checkErr(err)
			of[i].Write(shards[j])
			checkErr(err)
		}
		if int64(b) < stripeSize {
			break
		}
		stripeno++
	}
	for i := range of {
		of[i].Close()
	}
	//create a file and store the metainfo

	fmt.Println("simple encoder time spent:", time.Now().Sub(startTime))

}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(2)
	}
}
