// Strong consistency immutable key value store using the paxos algorithm, all
// from scratch. Storage is on disk. Runs several instances server side. As long
// as the majority are running, the store functions.
//
//     $ go run main.go --addr 188.226.130.53:10000 --nodes '188.226.130.53:10001 188.226.130.53:10002 188.226.130.53:10003 188.226.130.53:10004' --key rsa-private-key.pem &
//     $ go run main.go --addr 188.226.130.53:10001 --nodes '188.226.130.53:10000 188.226.130.53:10002 188.226.130.53:10003 188.226.130.53:10004' --key rsa-private-key.pem &
//     $ go run main.go --addr 188.226.130.53:10002 --nodes '188.226.130.53:10001 188.226.130.53:10000 188.226.130.53:10003 188.226.130.53:10004' --key rsa-private-key.pem &
//     $ go run main.go --addr 188.226.130.53:10003 --nodes '188.226.130.53:10001 188.226.130.53:10002 188.226.130.53:10000 188.226.130.53:10004' --key rsa-private-key.pem &
//     $ go run main.go --addr 188.226.130.53:10004 --nodes '188.226.130.53:10001 188.226.130.53:10002 188.226.130.53:10003 188.226.130.53:10000' --key rsa-private-key.pem &
//
// Client side
//
//     $ curl -X POST -d "People are crazy" 'http://188.226.130.53:10000/3'
//     People are crazy
//     $ curl 'http://188.226.130.53:10001/3'
//     People are crazy
//     $ curl -X POST -d "Beer is good" 'http://188.226.130.53:10002/3' // 3 has already been written
//     People are crazy

package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"science/paxos"
)

const usagePrefix = `Runs a key value store

Usage: go run ./main.go [OPTIONS]

OPTIONS:
`

var (
	addrFlag    = flag.String("addr", "localhost:10000", "Address to listen and serve")
	nodesFlag   = flag.String("nodes", "localhost:10001 localhost:10002", "Remote nodes")
	keyFlag     = flag.String("key", "", "Path to RSA private key")
	profileFlag = flag.Bool("profile", false, "Profile memory and GC")
)

func main() {
	// Setup flags
	flag.Usage = func() {
		fmt.Fprint(os.Stdout, usagePrefix)
		flag.PrintDefaults()
	}
	flag.Parse()
	if *keyFlag == "" {
		log.Fatalf("Must specify private RSA key with --key")
	}

	// Setup auth
	const authorizationHeader = "Authorization"
	if !crypto.SHA512.Available() {
		log.Fatalf("SHA512 not available")
	}
	keyBytes, err := ioutil.ReadFile(*keyFlag)
	if err != nil {
		log.Fatalf("Could not read %s: %v", *keyFlag, err)
	}
	block := (*pem.Block)(nil)
	if block, _ = pem.Decode(keyBytes); block == nil {
		log.Panicf("Could not decode PEM")
	}
	keyIntf, err := x509.ParsePKCS8PrivateKey(keyBytes)
	if err != nil {
		keyIntf, err = x509.ParsePKCS1PrivateKey(keyBytes)
		if err != nil {
			log.Fatalf("%s is not an RSA private key", *keyFlag)
		}
	}
	privateKey, ok := keyIntf.(*rsa.PrivateKey)
	if !ok {
		log.Fatalf("%s is not an RSA private key", *keyFlag)
	}
	publicKey := privateKey.Public().(*rsa.PublicKey)

	// Setup network
	network := paxos.NewNetwork()
	prefix := *addrFlag + " "
	stdout := log.New(os.Stdout, prefix, log.LstdFlags)
	stderr := log.New(os.Stderr, prefix, log.LstdFlags)
	if false {
		network.SetLoggers(stdout, stderr)
	}
	for _, node := range strings.Fields(*nodesFlag) {
		channel := make(chan []byte)
		network.AddRemoteNode(node, channel)
		go func(node string, channel <-chan []byte) {
			for data := range channel {
				// Some simple auth
				hash := sha512.Sum512(data)
				signatureBytes, _ := rsa.SignPKCS1v15(nil, privateKey, crypto.SHA512, hash[:])
				signature := base64.RawURLEncoding.EncodeToString(signatureBytes)

				addr := fmt.Sprintf("http://%s/paxos", node)
				req, err := http.NewRequest("POST", addr, bytes.NewBuffer(data))
				if err != nil {
					stderr.Print(err)
					continue
				}
				req.Header.Set(authorizationHeader, signature)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					stderr.Print(err)
					continue
				}
				resp.Body.Close()
			}
		}(node, channel)
	}
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	channel := make(chan []byte)
	node := network.AddNode(*addrFlag, channel, paxos.DiskStorage(path.Join(cwd, *addrFlag)))

	if *profileFlag {
		go func() {
			for _ = range time.Tick(1000 * time.Millisecond) {
				memStats := runtime.MemStats{}
				runtime.ReadMemStats(&memStats)
				fmt.Printf("Routines: %d\n", runtime.NumGoroutine())
				fmt.Printf("Objects: %d\n", memStats.HeapObjects)
				fmt.Printf("Memory: %d\n", memStats.Alloc)
				fmt.Printf("NextGC: %d\n", memStats.NextGC)
			}
		}()
	}

	// Listen and serve
	err = http.ListenAndServe(*addrFlag, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		path = strings.TrimSuffix(path, "/")
		if path == "paxos" {
			msg, err := ioutil.ReadAll(r.Body)
			if err != nil {
				stderr.Print(err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			hash := sha512.Sum512(msg)
			signature, err := base64.RawURLEncoding.DecodeString(r.Header.Get(authorizationHeader))
			if err != nil {
				stderr.Print(err)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA512, hash[:], signature); err != nil {
				stderr.Print(err)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			go func() {
				channel <- msg
			}()
			w.Header().Set("Content-Type", "text/plain")
			return
		}
		key, err := strconv.ParseUint(path, 10, 0)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := []byte(nil), error(nil)
		switch r.Method {
		case "GET":
			resp, err = node.Read(ctx, key)
			if err != nil {
				stderr.Print(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if resp == nil {
				http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
				return
			}
		case "POST":
			value, err := ioutil.ReadAll(r.Body)
			if err != nil {
				stderr.Print(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			resp, err = node.Write(ctx, key, value)
			if err != nil {
				stderr.Print(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		default:
			stderr.Print(http.StatusText(http.StatusMethodNotAllowed))
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(resp)
	}))
	if err != nil {
		stderr.Fatal(err)
	}
}
