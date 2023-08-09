protoc  -I=../../../../config/pkg/api/proto/ \
        --proto_path=./ \
        --go_out=../ \
        --go_opt=paths=source_relative \
        --go-grpc_out=../ \
        --go-grpc_opt=paths=source_relative \
        --go_opt=Mconfig.proto=github.com/c12s/star/pkg/api \
        star.proto