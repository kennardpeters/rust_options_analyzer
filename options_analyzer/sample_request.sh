#!/bin/bash
arg1=$1

json=$(jq -n \
	--arg arg1 "$arg1" \
	'{"contract_name": $arg1 }')

#Without proto definition
grpcurl -plaintext -proto ./proto/contracts.proto \
	-d "$json" \
	'[::1]:50051' contracts.Contract.ServerStreamContract
