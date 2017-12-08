# make test
test:
	go test keymanager/keymanager_test.go
	go test tree/bplus_test.go
	go test tree/hashdb_test.go
	go test leaf/kademliadb_test.go
	go test client/client_test.go

# make testswarm
testswarm:
	go test ../swarm/storage/dbchunkstore_test.go
	go test ../swarm/enssimulation_test.go
	go test ../swarm/api/tcpip/server_test.go
