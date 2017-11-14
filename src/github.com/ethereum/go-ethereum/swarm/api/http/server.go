// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

/*
A simple http server interface to Swarm
*/
package http

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/crypto/nacl/box"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/rs/cors"
	"github.com/ethereum/go-ethereum/accounts/keystore"
    	"github.com/ethereum/go-ethereum/accounts"
    	"github.com/ethereum/go-ethereum/crypto"	
)

// ServerConfig is the basic configuration needed for the HTTP server and also
// includes CORS settings.
type ServerConfig struct {
	Addr       string
	CorsString string
}

type devicejson struct{
	DeviceID string `json:"deviceID,omitempty"`
	Email string `json:"email,omitempty"`
	Phone string `json:"phone,omitempty"`
	Email_sha256 string `json:"email_sha256,omitempty"`
	Phone_sha256 string `json:"phone_sha256,omitempty"`
}

type tablejson struct{
 	TableID string `json:"tableid,omitempty"`
	ID string `json:"id,omitempty"`
	Document string `json:"document,omitempty"`
}

// browser API for registering bzz url scheme handlers:
// https://developer.mozilla.org/en/docs/Web-based_protocol_handlers
// electron (chromium) api for registering bzz url scheme handlers:
// https://github.com/atom/electron/blob/master/docs/api/protocol.md

// starts up http server
func StartHttpServer(api *api.Api, config *ServerConfig) {
	var allowedOrigins []string
	for _, domain := range strings.Split(config.CorsString, ",") {
		allowedOrigins = append(allowedOrigins, strings.TrimSpace(domain))
	}
	c := cors.New(cors.Options{
		AllowedOrigins: allowedOrigins,
		AllowedMethods: []string{"POST", "GET", "DELETE", "PATCH", "PUT"},
		MaxAge:         600,
		AllowedHeaders: []string{"*"},
	})
	sk, pk := GetKeys()
	hdlr := c.Handler(NewServer(api, sk, pk))

	go http.ListenAndServe(config.Addr, hdlr)
}

func GetKeys() (sk [32]byte, pk [32]byte) {
	ks := keystore.NewKeyStore("/var/www/vhosts/data/keystore", keystore.StandardScryptN, keystore.StandardScryptP) 
	var ks_accounts []accounts.Account      //     type Account struct    in->   keystore/keystore.go
	ks_accounts = ks.Accounts()   
	acc_url := ks_accounts[0].URL   
	acc_url_string := fmt.Sprintf("%s", acc_url)
	filename := acc_url_string[11:]  // /var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159
            
    // Open the key file
    //keyJson, readErr := ioutil.ReadFile("/var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159")
    keyJson, readErr := ioutil.ReadFile(filename)    
    if readErr != nil {
        //s.logDebug("SWARM server.go ReadFile of keystore file error: %s ", readErr)
        log.Debug(fmt.Sprintf("[BZZ] HTTP: "+"SWARM server.go ReadFile of keystore file error: %s ", readErr))
        
        // if ReadFile fail use default keys
        sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
		pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}
		return sk, pk        
    }
	
    keyWrapper, keyErr := keystore.DecryptKey([]byte(keyJson), "mdotm")
    if keyErr != nil {
        //s.logDebug("SWARM server.go DecryptKey error: %s ", keyErr)
        log.Debug(fmt.Sprintf("[BZZ] HTTP: "+"SWARM server.go DecryptKey error: %s ", keyErr))
        
        // if we don't know the pass use default keys
        sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
		pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}
		return sk, pk
    }
    
    acc_sk := crypto.FromECDSA(keyWrapper.PrivateKey)
    
    acc_pk :=   crypto.FromECDSAPub(&keyWrapper.PrivateKey.PublicKey)
    // fun call elliptic.Marshal   add  ret[0] = 4 // uncompressed point 
    // pk:[]byte{0x4, 0x8d, 0x9b,
    // need to remove the "ret[0] = 4" to get the pk
    acc_pk = append(pk[:0], pk[1:]...)
    
    //secretkey := [32]byte{}
    for i := range acc_sk {
          sk[i] = acc_sk[i]
          if i == 31 {break}  
    }
    
    // crypto/nacl  box.Seal()  box.Open()   PublicKey is  type *[32]byte  so cut the account PublicKey from [64]byte to [32]byte
    //publickey := [64]byte{}
    //publickey := [32]byte{}
    for i := range acc_pk {
          pk[i] = acc_pk[i]
          if i == 31 {break} 
    }    
    	
//	sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
//	pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}
	
	return sk, pk
}

func NewServer(api *api.Api, sk [32]byte, pk [32]byte) *Server {
	return &Server{api, sk, pk}
}

type Server struct {
	api *api.Api
	sk [32]byte
	pk [32]byte
}

// Request wraps http.Request and also includes the parsed bzz URI
type Request struct {
	http.Request

	uri *api.URI
}

// HandlePostRaw handles a POST request to a raw bzzr:/ URI, stores the request
// body in swarm and returns the resulting storage key as a text/plain response
func (s *Server) HandlePostRaw(w http.ResponseWriter, r *Request) {
	if r.uri.Path != "" {
		s.BadRequest(w, r, "raw POST request cannot contain a path")
		return
	}

	if r.Header.Get("Content-Length") == "" {
		s.BadRequest(w, r, "missing Content-Length header in request")
		return
	}

	bodycontent, _ := ioutil.ReadAll(r.Body)	
	rdrUpdated := ioutil.NopCloser(bytes.NewBuffer(bodycontent))
        r.ContentLength = int64(bytes.NewBuffer(bodycontent).Len())

	s.logDebug( fmt.Sprintf("%s ==> %+v with lenghth [%v]",bodycontent, bodycontent,r.ContentLength) )
	key, err := s.api.Store(rdrUpdated, r.ContentLength, nil)
	if err != nil {
		s.Error(w, r, err)
		return
	}
	reader_retrieved := s.api.Retrieve(key)
	s.logDebug("content for key.Log [%s] [%+v] stored [%+v]", key.Log(), key, reader_retrieved)

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, key)
}

func BuildSwarmdbPrefix(owner string, table string, id string) string {
	//hashType := "SHA3"
	//hashType := SHA256"

	//Should add checks for valid type / length for building
	prepString := strings.ToLower(owner) + strings.ToLower(table) + strings.ToLower(id)
	h256 := sha256.New()
        h256.Write([]byte(prepString))
        prefix := fmt.Sprintf("%x", h256.Sum(nil))
    	log.Debug(fmt.Sprintf("In BuildSwarmdbPrefix prepstring[%s] and prefix[%s] in Bytes [%v] with size [%v]" , prepString, prefix, []byte(prefix), len([]byte(prefix)) ) )
	return prefix
}

func (s *Server) HandlePostDB(w http.ResponseWriter, r *Request) {
    	log.Debug(fmt.Sprintf("In HandlePostDB r.uri(%v) r.uri.Path(%v) r.uri.Addr(%v)" ,r.uri, r.uri.Path, r.uri.Addr))

	//r.uri.Addr == Owner
	//r.uri.Path == table/id

    	if r.uri.Path == "" {
        	s.BadRequest(w, r, "DB POST request should contain a path")
        	return
    	}

    	if r.Header.Get("Content-Length") == "" {
        	s.BadRequest(w, r, "missing Content-Length header in request")
        	return
    	}

	rdrBody,_ := ioutil.ReadAll(r.Body)
	kv := string(rdrBody)
	s.logDebug("In HandlePostDB kv PRESTORE (%v) ", kv)
	kvlen := int64(len(kv))
    	dbwg := &sync.WaitGroup{}
    	rdb := strings.NewReader(kv)

	//Take the Hash returned for the stored 'Main' content and store it
	raw_indexkey, err := s.api.StoreDB(rdb, kvlen, dbwg)
    	if err != nil {
        	s.Error(w, r, err)
        	return
    	}
	s.logDebug("Index content stored (kv=[%v]) for raw_indexkey.Log [%s] [%+v] (size of [%+v])", string(kv), raw_indexkey.Log(), raw_indexkey, kvlen)

    	w.Header().Set("Content-Type", "text/plain")
    	w.WriteHeader(http.StatusOK)
    	fmt.Fprint(w, r.uri.Path)
}

func (s *Server) HandlePostHashDB(w http.ResponseWriter, r *Request) {
	k := r.uri.Path

	key, err := s.api.StoreHashDB([]byte(k), r.Body, r.ContentLength, nil)
	if err != nil {
		s.Error(w, r, err)
		return
	}
	s.logDebug("content for %s stored", key.Log())

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, key)
}

// HandlePostRaw handles a POST request to a raw bzzr:/ URI, stores the request
// body in swarm and returns the resulting storage key as a text/plain response
func (s *Server) HandlePostRawTest(w http.ResponseWriter, r *Request) {
    	log.Debug(fmt.Sprintf("In PostTest %v %v" ,r.uri.Path, r.uri.Addr))

    	if r.uri.Path != "" {
        	s.BadRequest(w, r, "raw POST request cannot contain a path")
        	return
    	}

    	if r.Header.Get("Content-Length") == "" {
        	s.BadRequest(w, r, "missing Content-Length header in request")
        	return
    	}
 
     	body, err := ioutil.ReadAll(r.Body)
 	var dec devicejson
 	json.Unmarshal(body, &dec)
     	log.Debug(fmt.Sprintf("In PostTest %v %v" ,dec, string(body)))
     	log.Debug(fmt.Sprintf("In PostTest body %v " ,string(body)))
 	var email, phone string
 	if len(dec.Email_sha256)>0{
 		email = dec.Email_sha256
 	}else if len(dec.Email) > 0{
 		h256_email := sha256.Sum256([]byte(strings.Trim(dec.Email, " ")))
 		email = hex.EncodeToString(h256_email[:])
 	}
 	if len(dec.Phone_sha256)>0{
 		phone = dec.Phone_sha256
	}else if len(dec.Phone) > 0{
 		h256 := sha256.New()
		h256.Write([]byte(dec.Phone))
 		phone = fmt.Sprintf("%x", h256.Sum(nil))
	}
    	log.Debug(fmt.Sprintf("In PostTest %v" ,dec))
    	log.Debug(fmt.Sprintf("In PostTest %v %v %v" ,dec.DeviceID, email, phone))
    	key, err := s.api.PutTest(string(body), "text/plain; charset=utf-8", dec.DeviceID, email, phone)
    	if err != nil {
        	s.Error(w, r, err)
        	return
    	}
	
    	s.logDebug("content for %s stored", key.Log())
	//s.api.ldb.Put(dec.DeviceID, key)

    	w.Header().Set("Content-Type", "text/plain")
    	w.WriteHeader(http.StatusOK)
    	fmt.Fprint(w, key)
}
 
func (s *Server) HandlePostRawTable(w http.ResponseWriter, r *Request) {
    body, err := ioutil.ReadAll(r.Body)
    var dec tablejson
    json.Unmarshal(body, &dec)

    log.Debug(fmt.Sprintf("In PostTable body %v %v" , dec, string(body)))
    log.Debug(fmt.Sprintf("In PostTable id %v tableid %v" , dec.ID, dec.TableID))
    log.Debug(fmt.Sprintf("In PostTable document %v" , dec.Document))

    key, err := s.api.PutTable(dec.Document, "text/plain; charset=utf-8", dec.ID, dec.TableID)
    if err != nil {
        s.Error(w, r, err)
        return
    }

    s.logDebug("content for %s stored", key.Log())

    w.Header().Set("Content-Type", "text/plain")
    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, key)
}


// HandlePostFiles handles a POST request (or deprecated PUT request) to
// bzz:/<hash>/<path> which contains either a single file or multiple files
// (either a tar archive or multipart form), adds those files either to an
// existing manifest or to a new manifest under <path> and returns the
// resulting manifest hash as a text/plain response
func (s *Server) HandlePostFiles(w http.ResponseWriter, r *Request) {
	contentType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		s.BadRequest(w, r, err.Error())
		return
	}

	var key storage.Key
	if r.uri.Addr != "" {
		key, err = s.api.Resolve(r.uri)
		if err != nil {
			s.Error(w, r, fmt.Errorf("error resolving %s: %s", r.uri.Addr, err))
			return
		}
	} else {
		key, err = s.api.NewManifest()
		if err != nil {
			s.Error(w, r, err)
			return
		}
	}

	newKey, err := s.updateManifest(key, func(mw *api.ManifestWriter) error {
		switch contentType {

		case "application/x-tar":
			return s.handleTarUpload(r, mw)

		case "multipart/form-data":
			return s.handleMultipartUpload(r, params["boundary"], mw)

		default:
			return s.handleDirectUpload(r, mw)
		}
	})
	if err != nil {
		s.Error(w, r, fmt.Errorf("error creating manifest: %s", err))
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, newKey)
}



func (s *Server) handleTarUpload(req *Request, mw *api.ManifestWriter) error {
	tr := tar.NewReader(req.Body)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error reading tar stream: %s", err)
		}

		// only store regular files
		if !hdr.FileInfo().Mode().IsRegular() {
			continue
		}

		// add the entry under the path from the request
		path := path.Join(req.uri.Path, hdr.Name)
		entry := &api.ManifestEntry{
			Path:        path,
			ContentType: hdr.Xattrs["user.swarm.content-type"],
			Mode:        hdr.Mode,
			Size:        hdr.Size,
			ModTime:     hdr.ModTime,
		}
		s.logDebug("adding %s (%d bytes) to new manifest", entry.Path, entry.Size)
		contentKey, err := mw.AddEntry(tr, entry)
		if err != nil {
			return fmt.Errorf("error adding manifest entry from tar stream: %s", err)
		}
		s.logDebug("content for %s stored", contentKey.Log())
	}
}

func (s *Server) handleMultipartUpload(req *Request, boundary string, mw *api.ManifestWriter) error {
	mr := multipart.NewReader(req.Body, boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error reading multipart form: %s", err)
		}

		var size int64
		var reader io.Reader = part
		if contentLength := part.Header.Get("Content-Length"); contentLength != "" {
			size, err = strconv.ParseInt(contentLength, 10, 64)
			if err != nil {
				return fmt.Errorf("error parsing multipart content length: %s", err)
			}
			reader = part
		} else {
			// copy the part to a tmp file to get its size
			tmp, err := ioutil.TempFile("", "swarm-multipart")
			if err != nil {
				return err
			}
			defer os.Remove(tmp.Name())
			defer tmp.Close()
			size, err = io.Copy(tmp, part)
			if err != nil {
				return fmt.Errorf("error copying multipart content: %s", err)
			}
			if _, err := tmp.Seek(0, os.SEEK_SET); err != nil {
				return fmt.Errorf("error copying multipart content: %s", err)
			}
			reader = tmp
		}

		// add the entry under the path from the request
		name := part.FileName()
		if name == "" {
			name = part.FormName()
		}
		path := path.Join(req.uri.Path, name)
		entry := &api.ManifestEntry{
			Path:        path,
			ContentType: part.Header.Get("Content-Type"),
			Size:        size,
			ModTime:     time.Now(),
		}
		s.logDebug("adding %s (%d bytes) to new manifest", entry.Path, entry.Size)
		contentKey, err := mw.AddEntry(reader, entry)
		if err != nil {
			return fmt.Errorf("error adding manifest entry from multipart form: %s", err)
		}
		s.logDebug("content for %s stored", contentKey.Log())
	}
}

func (s *Server) handleDirectUpload(req *Request, mw *api.ManifestWriter) error {
	key, err := mw.AddEntry(req.Body, &api.ManifestEntry{
		Path:        req.uri.Path,
		ContentType: req.Header.Get("Content-Type"),
		Mode:        0644,
		Size:        req.ContentLength,
		ModTime:     time.Now(),
	})
	if err != nil {
		return err
	}
	s.logDebug("content for %s stored", key.Log())
	return nil
}

// HandleDelete handles a DELETE request to bzz:/<manifest>/<path>, removes
// <path> from <manifest> and returns the resulting manifest hash as a
// text/plain response
func (s *Server) HandleDelete(w http.ResponseWriter, r *Request) {
	key, err := s.api.Resolve(r.uri)
	if err != nil {
		s.Error(w, r, fmt.Errorf("error resolving %s: %s", r.uri.Addr, err))
		return
	}

	newKey, err := s.updateManifest(key, func(mw *api.ManifestWriter) error {
		s.logDebug("removing %s from manifest %s", r.uri.Path, key.Log())
		return mw.RemoveEntry(r.uri.Path)
	})
	if err != nil {
		s.Error(w, r, fmt.Errorf("error updating manifest: %s", err))
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, newKey)
}

func (s *Server) HandleGetHashDB(w http.ResponseWriter, r *Request) {
	value := s.api.GetHashDB(r.uri.Path)
    log.Debug(fmt.Sprintf("HandleGetHashDB res %v %v" ,r.uri.Path, value))

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, value)
}

func (s *Server) HandleGetRawTest(w http.ResponseWriter, r *Request) {
	manifestroot := s.api.GetManifestRoot()
	if manifestroot == nil{
        s.logDebug("GetRawTest Manifest not found ")
        http.NotFound(w, &r.Request)
		return
	}
	r.uri.Addr = string(manifestroot)
    	log.Debug("GetRawTest Manifest = ", string(manifestroot))
    	log.Debug(fmt.Sprintf("In GetRawTest %v %v" ,r.uri.Path, r.uri.Addr))
	s.HandleGetRaw(w, r)
}

func (s *Server) HandleGetDB(w http.ResponseWriter, r *Request) {
    	log.Debug(fmt.Sprintf("In HandleGetDB r.uri(%v) r.uri.Path(%v) r.uri.Addr(%v)" , r.uri, r.uri.Path, r.uri.Addr))

	//r.uri.Addr == Owner
	//r.uri.Path == table/id

    	keylen := 64 ///////..........
    	dummy := bytes.Repeat([]byte("Z"), keylen)

	owner := r.uri.Addr
	table_id_parts := strings.Split(r.uri.Path, "/")
	table := table_id_parts[0]
	id := table_id_parts[1]
	contentPrefix := BuildSwarmdbPrefix(owner, table, id)	

    	newkeybase := contentPrefix+string(dummy)
    	chunker := storage.NewTreeChunker(storage.NewChunkerParams())
    	rd := strings.NewReader(newkeybase)
    	key, err := chunker.Split(rd, int64(len(newkeybase)), nil, nil, nil, false)
    	log.Debug(fmt.Sprintf("In HandleGetDB prefix [%v] dummy %v newkeybase %v key %v", contentPrefix, dummy, newkeybase, key))

    	contentReader := s.api.Retrieve(key)
    	if _, err := contentReader.Size(nil); err != nil {
        	s.logDebug("key not found %s: %s", key, err)
        	http.NotFound(w, &r.Request)
        	return
    	}
    	if err != nil {
        	s.Error(w, r, err)
        	return
    	}
	
	contentReaderSize,_ := contentReader.Size(nil)
	contentBytes := make( []byte, contentReaderSize )
 	_,_ = contentReader.ReadAt( contentBytes, 0 )
	
	encryptedContentBytes := contentBytes[len(contentPrefix):]
    	log.Debug(fmt.Sprintf("In HandledGetDB Retrieved 'mainhash' v[%v] s[%s] ", encryptedContentBytes, encryptedContentBytes))

        decrypted_reader := bytes.NewReader(s.DecryptData(encryptedContentBytes))
    	log.Debug(fmt.Sprintf("In HandledGetDB got back the 'reader' v[%v] s[%s] ", decrypted_reader, decrypted_reader))

    	// allow the request to overwrite the content type using a query
    	// parameter
    	contentType := "application/octet-stream"
    	if typ := r.URL.Query().Get("content_type"); typ != "" {
        	contentType = typ
    	}
	decryptedReaderSize := decrypted_reader.Size()
	queryResponse := make( []byte, decryptedReaderSize-416 ) //TODO: match to sizes in metadata content
	_,_ = decrypted_reader.ReadAt( queryResponse, 416 )  //TODO: match to sizes in metadata content
	queryResponseReader := bytes.NewReader( queryResponse )
    	w.Header().Set("Content-Type", contentType)
    	http.ServeContent(w, &r.Request, "", time.Now(), queryResponseReader)
}

func (s *Server) HandleGetRawTable(w http.ResponseWriter, r *Request) {
	id := r.URL.Query().Get("id")
	tableid := r.URL.Query().Get("tableid")
	key := tableid + "_" + id
    	//key, _ := s.api.Resolve(r.uri)
    	log.Debug(fmt.Sprintf("In GetTable %v %v %v" ,id, tableid, key))
    	reader, contentType, _, err := s.api.GetTest(key)
    	if err != nil {
        	s.Error(w, r, err)
        	return
    	}

    	// check the root chunk exists by retrieving the file's size
    	if _, err := reader.Size(nil); err != nil {
        	s.logDebug("file not found %s: %s", r.uri, err)
        	http.NotFound(w, &r.Request)
        	return
    	}

    	w.Header().Set("Content-Type", contentType)

    	http.ServeContent(w, &r.Request, "", time.Now(), reader)
    	log.Debug(fmt.Sprintf("GetTable %v, %v" ,r.uri.Path, reader))
}

// HandleGetRaw handles a GET request to bzzr://<key> and responds with
// the raw content stored at the given storage key
func (s *Server) HandleGetRaw(w http.ResponseWriter, r *Request) {
	key, err := s.api.Resolve(r.uri)
	log.Debug(fmt.Sprintf("In GetRaw %v %v %v %v",r.uri ,r.uri.Path, r.uri.Addr, key))
	if err != nil {
		s.Error(w, r, fmt.Errorf("error resolving %s: %s", r.uri.Addr, err))
		return
	}

	// if path is set, interpret <key> as a manifest and return the
	// raw entry at the given path
	if r.uri.Path != "" {
		walker, err := s.api.NewManifestWalker(key, nil)
		if err != nil {
			s.BadRequest(w, r, fmt.Sprintf("%s is not a manifest", key))
			return
		}
		var entry *api.ManifestEntry
		walker.Walk(func(e *api.ManifestEntry) error {
			// if the entry matches the path, set entry and stop
			// the walk
			if e.Path == r.uri.Path {
				entry = e
				// return an error to cancel the walk
				return errors.New("found")
			}

			// ignore non-manifest files
			if e.ContentType != api.ManifestType {
				return nil
			}

			// if the manifest's path is a prefix of the
			// requested path, recurse into it by returning
			// nil and continuing the walk
			if strings.HasPrefix(r.uri.Path, e.Path) {
				return nil
			}

			return api.SkipManifest
		})
		if entry == nil {
			http.NotFound(w, &r.Request)
			return
		}
		key = storage.Key(common.Hex2Bytes(entry.Hash))
	}

	// check the root chunk exists by retrieving the file's size
	reader := s.api.Retrieve(key)
	readerSize,_ := reader.Size(nil)
	encrypted_reader := make([]byte, readerSize )
	_,_ = reader.ReadAt(encrypted_reader,0)
	s.logDebug("Retrieve Raw encrypted data of [%+v] ==> [%s] using key [%+v]", encrypted_reader, encrypted_reader, key)
	decrypted_reader := bytes.NewReader(s.DecryptData(encrypted_reader))
	if _, err := reader.Size(nil); err != nil {
		s.logDebug("key not found %s: %s", key, err)
		http.NotFound(w, &r.Request)
		return
	}

	// allow the request to overwrite the content type using a query
	// parameter
	contentType := "application/octet-stream"
	if typ := r.URL.Query().Get("content_type"); typ != "" {
		contentType = typ
	}
	w.Header().Set("Content-Type", contentType)

	http.ServeContent(w, &r.Request, "", time.Now(), decrypted_reader)
}

// HandleGetFiles handles a GET request to bzz:/<manifest> with an Accept
// header of "application/x-tar" and returns a tar stream of all files
// contained in the manifest
func (s *Server) HandleGetFiles(w http.ResponseWriter, r *Request) {
	if r.uri.Path != "" {
		s.BadRequest(w, r, "files request cannot contain a path")
		return
	}

	key, err := s.api.Resolve(r.uri)
	log.Debug(fmt.Sprintf("In GetFiles %v %v %v" ,r.uri.Path, r.uri.Addr, key))
	if err != nil {
		s.Error(w, r, fmt.Errorf("error resolving %s: %s", r.uri.Addr, err))
		return
	}

	walker, err := s.api.NewManifestWalker(key, nil)
	if err != nil {
		s.Error(w, r, err)
		return
	}

	tw := tar.NewWriter(w)
	defer tw.Close()
	w.Header().Set("Content-Type", "application/x-tar")
	w.WriteHeader(http.StatusOK)

	err = walker.Walk(func(entry *api.ManifestEntry) error {
		// ignore manifests (walk will recurse into them)
		if entry.ContentType == api.ManifestType {
			return nil
		}

		// retrieve the entry's key and size
		reader := s.api.Retrieve(storage.Key(common.Hex2Bytes(entry.Hash)))
		size, err := reader.Size(nil)
		if err != nil {
			return err
		}

		// write a tar header for the entry
		hdr := &tar.Header{
			Name:    entry.Path,
			Mode:    entry.Mode,
			Size:    size,
			ModTime: entry.ModTime,
			Xattrs: map[string]string{
				"user.swarm.content-type": entry.ContentType,
			},
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		// copy the file into the tar stream
		n, err := io.Copy(tw, io.LimitReader(reader, hdr.Size))
		if err != nil {
			return err
		} else if n != size {
			return fmt.Errorf("error writing %s: expected %d bytes but sent %d", entry.Path, size, n)
		}

		return nil
	})
	if err != nil {
		s.logError("error generating tar stream: %s", err)
	}
}

// HandleGetList handles a GET request to bzz:/<manifest>/<path> which has
// the "list" query parameter set to "true" and returns a list of all files
// contained in <manifest> under <path> grouped into common prefixes using
// "/" as a delimiter
func (s *Server) HandleGetList(w http.ResponseWriter, r *Request) {
	// ensure the root path has a trailing slash so that relative URLs work
	if r.uri.Path == "" && !strings.HasSuffix(r.URL.Path, "/") {
		http.Redirect(w, &r.Request, r.URL.Path+"/?list=true", http.StatusMovedPermanently)
		return
	}

	key, err := s.api.Resolve(r.uri)
	if err != nil {
		s.Error(w, r, fmt.Errorf("error resolving %s: %s", r.uri.Addr, err))
		return
	}

	walker, err := s.api.NewManifestWalker(key, nil)
	if err != nil {
		s.Error(w, r, err)
		return
	}

	var list api.ManifestList
	prefix := r.uri.Path
	err = walker.Walk(func(entry *api.ManifestEntry) error {
		// handle non-manifest files
		if entry.ContentType != api.ManifestType {
			// ignore the file if it doesn't have the specified prefix
			if !strings.HasPrefix(entry.Path, prefix) {
				return nil
			}

			// if the path after the prefix contains a slash, add a
			// common prefix to the list, otherwise add the entry
			suffix := strings.TrimPrefix(entry.Path, prefix)
			if index := strings.Index(suffix, "/"); index > -1 {
				list.CommonPrefixes = append(list.CommonPrefixes, prefix+suffix[:index+1])
				return nil
			}
			if entry.Path == "" {
				entry.Path = "/"
			}
			list.Entries = append(list.Entries, entry)
			return nil
		}

		// if the manifest's path is a prefix of the specified prefix
		// then just recurse into the manifest by returning nil and
		// continuing the walk
		if strings.HasPrefix(prefix, entry.Path) {
			return nil
		}

		// if the manifest's path has the specified prefix, then if the
		// path after the prefix contains a slash, add a common prefix
		// to the list and skip the manifest, otherwise recurse into
		// the manifest by returning nil and continuing the walk
		if strings.HasPrefix(entry.Path, prefix) {
			suffix := strings.TrimPrefix(entry.Path, prefix)
			if index := strings.Index(suffix, "/"); index > -1 {
				list.CommonPrefixes = append(list.CommonPrefixes, prefix+suffix[:index+1])
				return api.SkipManifest
			}
			return nil
		}

		// the manifest neither has the prefix or needs recursing in to
		// so just skip it
		return api.SkipManifest
	})
	if err != nil {
		s.Error(w, r, err)
		return
	}

	// if the client wants HTML (e.g. a browser) then render the list as a
	// HTML index with relative URLs
	if strings.Contains(r.Header.Get("Accept"), "text/html") {
		w.Header().Set("Content-Type", "text/html")
		err := htmlListTemplate.Execute(w, &htmlListData{
			URI:  r.uri,
			List: &list,
		})
		if err != nil {
			s.logError("error rendering list HTML: %s", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&list)
}

// HandleGetFile handles a GET request to bzz://<manifest>/<path> and responds
// with the content of the file at <path> from the given <manifest>
func (s *Server) HandleGetFile(w http.ResponseWriter, r *Request) {
	key, err := s.api.Resolve(r.uri)
	log.Debug(fmt.Sprintf("In GetFile %v %v %v" ,r.uri.Path, r.uri.Addr, key))
	if err != nil {
		s.Error(w, r, fmt.Errorf("error resolving %s: %s", r.uri.Addr, err))
		return
	}

	reader, contentType, _, err := s.api.Get(key, r.uri.Path)
	if err != nil {
		s.Error(w, r, err)
		return
	}

	// check the root chunk exists by retrieving the file's size
	if _, err := reader.Size(nil); err != nil {
		s.logDebug("file not found %s: %s", r.uri, err)
		http.NotFound(w, &r.Request)
		return
	}

	w.Header().Set("Content-Type", contentType)

	http.ServeContent(w, &r.Request, "", time.Now(), reader)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.logDebug("HTTP %s request URL: '%s', Host: '%s', Path: '%s', Referer: '%s', Accept: '%s'", r.Method, r.RequestURI, r.URL.Host, r.URL.Path, r.Referer(), r.Header.Get("Accept"))
	uri, err := api.Parse(strings.TrimLeft(r.URL.Path, "/"))
	if err != nil {
		s.logError("Invalid URI %q: %s", r.URL.Path, err)
		http.Error(w, fmt.Sprintf("Invalid bzz URI: %s", err), http.StatusBadRequest)
		return
	}

	if uri.Swarmdb() == true {	
		bodycontent,_ := ioutil.ReadAll(r.Body)
		//Need to determine how to collect the following and attach to chunk 
		ownerAddress := []byte(strings.ToLower(uri.Addr))
		buyAt := []byte("4096000000000000") //Need to research how to grab 
		timestamp := []byte(strconv.FormatInt(time.Now().Unix(),10))
		blockNumber := []byte("100")

	        path_parts := strings.Split(uri.Path, "/")
        	table := strings.ToLower(path_parts[0])
        	id := strings.ToLower(path_parts[1])
        	contentPrefix := BuildSwarmdbPrefix(string(ownerAddress), table, id)

		var metadataBody []byte
		metadataBody = make([]byte, 108)
		copy(metadataBody[0:41], ownerAddress)
		copy(metadataBody[42:59], buyAt)
		copy(metadataBody[60:91], blockNumber)
		copy(metadataBody[92:107], timestamp)
		//metadataBody := []byte(string(ownerAddress) + string(buyAt) + string(timestamp) + string(blockNumber)) 
		//End of metadata chunk append	
		encryptedBodycontent := s.EncryptData( bodycontent )
		var mergedBodycontent []byte
		mergedBodycontent = make([]byte, 4096)
		//mergedBodycontent := []byte(string(metadataBody) + string(encryptedBodycontent))
		copy(mergedBodycontent[:], metadataBody) 
		copy(mergedBodycontent[512:576], contentPrefix)
		copy(mergedBodycontent[577:], encryptedBodycontent)

		s.logDebug("MetadataBody: [%v][%s]", metadataBody, string(metadataBody))
		s.logDebug("ContentPrefix: [%s]", string(contentPrefix))
		s.logDebug("RAW Content vs encryptedContent: [%v][%s]", bodycontent, string(encryptedBodycontent))

		mergedBodyContentReader := ioutil.NopCloser(bytes.NewBuffer(mergedBodycontent))
		r.Body = mergedBodyContentReader
		r.ContentLength = int64(bytes.NewBuffer(mergedBodycontent).Len())
		if( r.ContentLength > int64(4096) && uri.Swarmdb() == true ) {
			http.Error(w, "ContentLength "+strconv.Itoa(int(r.ContentLength))+" is longer than 4096 limit.", http.StatusBadRequest)
		}
	}

	req := &Request{Request: *r, uri: uri}
	switch r.Method {
	case "POST":
		s.logDebug("server POST %s %s", uri, req.uri.Addr)
		if req.uri.Addr == "demo" || r.URL.Query().Get("posttest") == "true"{
			s.HandlePostRawTest(w, req)
			return
		}
		if req.uri.Addr == "table" {
            		s.HandlePostRawTable(w, req)
            		return
		}
		if req.uri.Addr == "hashdb" {
			s.HandlePostHashDB(w, req)
			return
		}
		if uri.Swarmdb() == true {
            		s.HandlePostDB(w, req)
            		return
		}
		if uri.Raw() {
			s.HandlePostRaw(w, req)
		} else {
			s.HandlePostFiles(w, req)
		}

	case "PUT":
		// DEPRECATED:
		//   clients should send a POST request (the request creates a
		//   new manifest leaving the existing one intact, so it isn't
		//   strictly a traditional PUT request which replaces content
		//   at a URI, and POST is more ubiquitous)
		if uri.Raw() {
			http.Error(w, fmt.Sprintf("No PUT to %s allowed.", uri), http.StatusBadRequest)
			return
		} else {
			s.HandlePostFiles(w, req)
		}

	case "DELETE":
		if req.uri.Addr == "hashdb" {
			//will come later
			//s.HandleDeleteHashDB(w, req)
			return
		}
		if uri.Raw() {
			http.Error(w, fmt.Sprintf("No DELETE to %s allowed.", uri), http.StatusBadRequest)
			return
		}
		s.HandleDelete(w, req)

	case "GET":
		s.logDebug("server GET %s %s [%s]", uri, r.URL.Query(), uri.Swarmdb())
		if uri.Swarmdb() == true {
			s.HandleGetDB(w, req)
			return
		}
		if req.uri.Addr == "demo" || r.URL.Query().Get("gettest") == "true"{
			s.HandleGetRawTest(w, req)
			return
		}
		if req.uri.Addr == "table" {
			s.HandleGetRawTable(w, req)
			return
		}
		if req.uri.Addr == "hashdb" {
			s.HandleGetHashDB(w, req)
			return
		}
		if uri.Raw() {
			s.HandleGetRaw(w, req)
			return
		}

		if r.Header.Get("Accept") == "application/x-tar" {
			s.HandleGetFiles(w, req)
			return
		}

		if r.URL.Query().Get("list") == "true" {
			s.HandleGetList(w, req)
			return
		}

		s.HandleGetFile(w, req)

	default:
		http.Error(w, "Method "+r.Method+" is not supported.", http.StatusMethodNotAllowed)

	}
}

func (s *Server) updateManifest(key storage.Key, update func(mw *api.ManifestWriter) error) (storage.Key, error) {
	mw, err := s.api.NewManifestWriter(key, nil)
	if err != nil {
		return nil, err
	}

	if err := update(mw); err != nil {
		return nil, err
	}

	key, err = mw.Store()
	if err != nil {
		return nil, err
	}
	s.logDebug("generated manifest %s", key)
	return key, nil
}

func (s *Server) DecryptData( data []byte ) []byte { 
	var decryptNonce [24]byte
	//decryptNonce = [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
	copy(decryptNonce[:], data[:24])
	decrypted, ok := box.Open(nil, data[24:], &decryptNonce, &s.pk, &s.sk)
	if !ok {
		panic("decryption error")
	}
	return decrypted
}

func (s *Server) EncryptData( data []byte ) []byte { 
	var nonce [24]byte
	nonce = [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
	msg := data //[]byte("Alas, poor Yorick! I knew him, Horatio")
	encrypted := box.Seal(nonce[:], msg, &nonce, &s.pk, &s.sk)
	return encrypted
}

func (s *Server) logDebug(format string, v ...interface{}) {
	log.Debug(fmt.Sprintf("[BZZ] HTTP: "+format, v...))
}

func (s *Server) logError(format string, v ...interface{}) {
	log.Error(fmt.Sprintf("[BZZ] HTTP: "+format, v...))
}

func (s *Server) BadRequest(w http.ResponseWriter, r *Request, reason string) {
	s.logDebug("bad request %s %s: %s", r.Method, r.uri, reason)
	http.Error(w, reason, http.StatusBadRequest)
}

func (s *Server) Error(w http.ResponseWriter, r *Request, err error) {
	s.logError("error serving %s %s: %s", r.Method, r.uri, err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
