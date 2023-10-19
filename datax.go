package datax

import (
	"encoding/json"
	"fmt"
	"github.com/ebitengine/purego"
	"github.com/fxamacker/cbor/v2"
	"os"
	"runtime"
	"unsafe"
)

type DataX struct {
	initialize func()
	next       func() uintptr
	emit       func(unsafe.Pointer, int32, string)

	messageClose     func(uintptr)
	messageReference func(uintptr) string
	messageStream    func(uintptr) string
	messageData      func(uintptr) unsafe.Pointer
	messageDataSize  func(uintptr) int32
}

func libraryName() string {
	switch runtime.GOOS {
	case "darwin":
		return "libdatax-sdk.dylib"
	case "linux":
		return "libdatax-sdk.so"
	default:
		panic(fmt.Errorf("GOOS=%s is not supported", runtime.GOOS))
	}
}

func New() (*DataX, error) {
	library := libraryName()
	sdkHandle, err := purego.Dlopen(library, purego.RTLD_LOCAL|purego.RTLD_LAZY)
	if err != nil {
		return nil, fmt.Errorf("%w loading %s", err, library)
	}

	dx := &DataX{}

	purego.RegisterLibFunc(&dx.initialize, sdkHandle, "datax_sdk_v3_initialize")
	purego.RegisterLibFunc(&dx.next, sdkHandle, "datax_sdk_v3_next")
	purego.RegisterLibFunc(&dx.emit, sdkHandle, "datax_sdk_v3_emit")
	purego.RegisterLibFunc(&dx.messageClose, sdkHandle, "datax_sdk_v3_message_close")
	purego.RegisterLibFunc(&dx.messageReference, sdkHandle, "datax_sdk_v3_message_reference")
	purego.RegisterLibFunc(&dx.messageStream, sdkHandle, "datax_sdk_v3_message_stream")
	purego.RegisterLibFunc(&dx.messageData, sdkHandle, "datax_sdk_v3_message_data")
	purego.RegisterLibFunc(&dx.messageDataSize, sdkHandle, "datax_sdk_v3_message_data_size")

	dx.initialize()

	return dx, nil
}

func (dx *DataX) Close() {
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

func (dx *DataX) Next(msg interface{}) (stream, reference string, err error) {
	handle := dx.next()
	stream = dx.messageStream(handle)
	reference = dx.messageReference(handle)
	dataSize := dx.messageDataSize(handle)
	dataPtr := dx.messageData(handle)
	data := unsafe.Slice((*byte)(dataPtr), dataSize)
	err = cbor.Unmarshal(data, msg)
	if err != nil {
		err = fmt.Errorf("%w decoding input message")
	}
	dx.messageClose(handle)
	return
}

func (dx *DataX) Emit(msg interface{}, reference ...string) error {
	ref := ""
	if len(reference) > 0 {
		ref = reference[0]
	}
	data, err := cbor.Marshal(msg)
	if err != nil {
		return fmt.Errorf("%w marshaling message data", err)
	}
	dx.emit(unsafe.Pointer(&data[0]), int32(len(data)), ref)
	return nil
}

// EmitWithReference
// Deprecated, use Emit()
func (dx *DataX) EmitWithReference(msg interface{}, reference string) error {
	return dx.Emit(msg, reference)
}
