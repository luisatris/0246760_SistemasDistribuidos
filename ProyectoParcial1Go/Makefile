# Esto nos ayuda a posicionar nuestros config files en una carpeta dentro de nuestro proyecto

CONFIG_PATH=${HOME}/Documents/0246760_SistemasDistribuidos/ProyectoParcial1Go

.PHONY: init

init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
# gencert
# First creates the bare certificate, it is just the base certificate that others will differ from
# Then creates the server certificate, this allows our server certification
# Finally we create the client certificate this allows two way authentication
gencert:
	cfssl gencert \
		-initca internal/test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=internal/test/ca-config.json \
		-profile=server \
		internal/test/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=internal/test/ca-config.json \
		-profile=client \
		internal/test/client-csr.json | cfssljson -bare client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=internal/test/ca-config.json \
		-profile=client \
		-cn="root" \
		internal/test/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=internal/test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		internal/test/client-csr.json | cfssljson -bare nobody-client

	# Mover archivos generados a la carpeta interna/test
	mv ca.pem ca-key.pem ca.csr server*.pem server*.csr client*.pem client*.csr internal/test/

compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.

$(CONFIG_PATH)/model.conf:
	cp internal/test/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp internal/test/policy.csv $(CONFIG_PATH)/policy.csv

test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	go test -race ./...

compile_rpc:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
