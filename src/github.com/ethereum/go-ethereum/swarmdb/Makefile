
.PHONY: all enssimulation dbchunkstore server keymanager bplus client kademliadb hashdb

all:
	@echo "test all."
	@echo "test enssimulation."
	-go test ./enssimulation_test.go
	@echo "test dbchunkstore."
	-go test ./dbchunkstore_test.go
	@echo "test server."
	-go test ./server/server_test.go
	@echo "test keymanager."
	-go test ./keymanager/keymanager_test.go
	@echo "test bplus."
	-go test ./bplus_test.go
	@echo "test client."
	-go test ./client_test.go
	@echo "test kademliadb."
	-go test ./kademliadb_test.go
	@echo "test hashdb."
	-go test ./hashdb_test.go
	@echo "test netstat."
	-go test ./netStat_test.go

enssimulation:
	@echo "test enssimulation."
	go test ./enssimulation_test.go

dbchunkstore:
	@echo "test dbchunkstore."
	go test ./dbchunkstore_test.go

server:
	@echo "test server."
	go test ../swarm/api/tcpip/server_test.go

keymanager:
	@echo "test keymanager."
	go test ./keymanager/keymanager_test.go

bplus:
	@echo "test bplus."
	go test bplus_test.go

client:
	@echo "test client."
	go test client_test.go

kademliadb:
	@echo "test kademliadb."
	go test kademliadb_test.go

hashdb:
	@echo "test hashdb."
	go test hashdb_test.go

netstat:
	@echo "test netstat."
	go test ./netStat_test.go
