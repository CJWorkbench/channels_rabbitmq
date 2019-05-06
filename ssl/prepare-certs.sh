#!/bin/bash

set -e
set -x

cd "$(dirname "$0")"

rm -f ./*.{srl,csr,key,cert}

# root-CA ("Certificate authority") key
openssl genrsa -out ca.key 2048
# root-CA certificate (server and client will trust this)
openssl req -x509 -new -key ca.key -sha256 -days 9999 -out ca.cert -subj '/CN=localhost'

# server key
openssl genrsa -out server.key 2048
# server CSR (certificate signing request)
openssl req -new -key server.key -subj '/CN=localhost' -out server.csr
# server certificate (signed by root CA)
openssl x509 -req -in server.csr -CA ca.cert -CAkey ca.key -CAcreateserial -out server.cert -days 9999 -sha256

# server certificate must be readable in Docker container
chmod 0644 server.key

# client key
openssl genrsa -out client.key 2048
# client CSR (certificate signing request)
openssl req -new -key client.key -subj '/CN=localhost' -out client.csr
# client certificate (signed by root CA)
openssl x509 -req -in client.csr -CA ca.cert -CAkey ca.key -CAcreateserial -out client.cert -days 9999 -sha256
# Python wants a "certificate chain" that includes the root CA
cat client.cert ca.cert > client.certchain
