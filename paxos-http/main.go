// http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf
//
// Strongly consistent immutable key value store using the paxos algorithm, all from
// scratch. Storage is on disk. Runs several instances server side. As long as the majority are
// running, the store functions.
//
//     $ . ./gopath.sh
//     $ go run ./paxos-http/main.go --addr 188.226.130.53:10000 --nodes '188.226.130.53:10001 188.226.130.53:10002 188.226.130.53:10003 188.226.130.53:10004' &
//     $ go run ./paxos-http/main.go --addr 188.226.130.53:10001 --nodes '188.226.130.53:10000 188.226.130.53:10002 188.226.130.53:10003 188.226.130.53:10004' &
//     $ go run ./paxos-http/main.go --addr 188.226.130.53:10002 --nodes '188.226.130.53:10001 188.226.130.53:10000 188.226.130.53:10003 188.226.130.53:10004' &
//     $ go run ./paxos-http/main.go --addr 188.226.130.53:10003 --nodes '188.226.130.53:10001 188.226.130.53:10002 188.226.130.53:10000 188.226.130.53:10004' &
//     $ go run ./paxos-http/main.go --addr 188.226.130.53:10004 --nodes '188.226.130.53:10001 188.226.130.53:10002 188.226.130.53:10003 188.226.130.53:10000' &
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
	addrFlag  = flag.String("addr", "localhost:10000", "Address to listen and serve")
	nodesFlag = flag.String("nodes", "localhost:10001 localhost:10002", "Remote nodes")
)

func main() {
	// Setup flags
	flag.Usage = func() {
		fmt.Fprint(os.Stdout, usagePrefix)
		flag.PrintDefaults()
	}
	flag.Parse()

	// Setup network
	network := paxos.NewNetwork()
	prefix := *addrFlag + " "
	stdout := log.New(os.Stdout, prefix, log.LstdFlags)
	stderr := log.New(os.Stderr, prefix, log.LstdFlags)
	paxos.SetLoggers(network, stdout, stderr)
	for _, node := range strings.Fields(*nodesFlag) {
		channel := make(chan []byte)
		paxos.AddRemoteNode(network, node, channel)
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
	node := paxos.NewNode(*addrFlag, channel, network, paxos.DiskStorage(path.Join(cwd, *addrFlag)))
	paxos.AddNode(network, node)

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
			resp, err = paxos.Read(ctx, key, node)
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
			resp, err = paxos.Write(ctx, key, value, node)
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

var (
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
)

const authorizationHeader = "Authorization"

func init() {
	if !crypto.SHA512.Available() {
		log.Panicf("SHA512 not available")
	}
	const pemString = `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----`
	block := (*pem.Block)(nil)
	if block, _ = pem.Decode([]byte(pemString)); block == nil {
		log.Panicf("Could not decode PEM")
	}
	var err error
	privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		log.Panicf("Could parse PKCS1: %v", err)
	}
	publicKey = privateKey.Public().(*rsa.PublicKey)
}
