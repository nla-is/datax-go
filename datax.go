package datax

import (
	"context"
	"encoding/json"
	sdkprotocolv1 "github.com/nla-is/datax-sdk-protocol/v1"
	"github.com/sevlyar/retag"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
	"os"
)

type DataX struct {
	clientConn *grpc.ClientConn
	sdkClient  sdkprotocolv1.DataXClient
	tagger     *tagger
}

func New() (*DataX, error) {
	sidecarAddress := os.Getenv("DATAX_SIDECAR_ADDRESS")
	if sidecarAddress == "" {
		sidecarAddress = "127.0.0.1:20001"
	}
	clientConn, err := grpc.Dial(sidecarAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	sdkClient := sdkprotocolv1.NewDataXClient(clientConn)

	dx := &DataX{
		clientConn: clientConn,
		sdkClient:  sdkClient,
		tagger:     newTagger(),
	}

	return dx, nil
}

func (dx *DataX) Close() {
	if dx.clientConn != nil {
		_ = dx.clientConn.Close()
	}
}

func (dx *DataX) Configuration(cfg interface{}) error {
	configurationPath := os.Getenv("DATAX_CONFIGURATION")
	if configurationPath == "" {
		configurationPath = "/datax/configuration"
	}
	data, err := os.ReadFile(configurationPath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, cfg)
}

func (dx *DataX) NextRaw() (stream, reference string, data []byte, err error) {
	response, err := dx.sdkClient.Next(context.Background(), &sdkprotocolv1.NextOptions{})
	if err != nil {
		return "", "", nil, err
	}
	return response.Stream, response.Reference, response.Data, nil
}

func (dx *DataX) Next(msg interface{}) (stream, reference string, err error) {
	var data []byte
	if stream, reference, data, err = dx.NextRaw(); err != nil {
		return "", "", err
	}
	if err = msgpack.Unmarshal(data, msg); err != nil {
		return "", "", err
	}
	return stream, reference, nil
}

func (dx *DataX) emit(request *sdkprotocolv1.EmitMessage) error {
	_, err := dx.sdkClient.Emit(context.Background(), request)
	return err
}

func (dx *DataX) EmitRawWithReference(data []byte, reference string) error {
	return dx.emit(&sdkprotocolv1.EmitMessage{
		Data:      data,
		Reference: &reference,
	})
}

func (dx *DataX) EmitRaw(data []byte) error {
	return dx.emit(&sdkprotocolv1.EmitMessage{
		Data: data,
	})
}

func (dx *DataX) encode(msg interface{}) ([]byte, error) {
	dxMsg := retag.ConvertAny(msg, dx.tagger)
	return msgpack.Marshal(dxMsg)
}

func (dx *DataX) Emit(msg interface{}) error {
	data, err := dx.encode(msg)
	if err != nil {
		return err
	}
	return dx.EmitRaw(data)
}

func (dx *DataX) EmitWithReference(msg interface{}, stream string) error {
	data, err := dx.encode(msg)
	if err != nil {
		return err
	}
	return dx.EmitRawWithReference(data, stream)
}
