#!/bin/bash
if [ $1 == "basis" ]; then
	go test -run FailNoAgree
	go test -run ConcurrentStarts
	go test -run Rejoin
	go test -run Backup
elif [ $1 == "persist" ]; then
  go test -run Persist1
	go test -run Persist2
  go test -run Persist3
elif [ $1 == "extended" ]; then
  go test -run Figure8
	go test -run UnreliableAgree
  go test -run Figure8Unreliable
	go test -run ReliableChurn
	go test -run UnreliableChurn
fi
