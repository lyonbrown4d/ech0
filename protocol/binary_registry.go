package protocol

import (
	"slices"

	"github.com/samber/oops"
)

type bodyEncoder func(any) ([]byte, error)
type bodyDecoder func([]byte, any) error

type bodyCodecEntry struct {
	command uint16
	encode  bodyEncoder
	decode  bodyDecoder
}

type bodyCodecRegistry struct {
	encoders map[uint16]bodyEncoder
	decoders map[uint16]bodyDecoder
	commands []uint16
}

var bodyCodecs = newBodyCodecRegistry()

func EncodeBody(command uint16, value any) ([]byte, error) {
	encoder, ok := bodyCodecs.encoders[command]
	if !ok {
		return nil, unsupportedCommand(command)
	}
	return encoder(value)
}

func DecodeBody(command uint16, data []byte, target any) error {
	decoder, ok := bodyCodecs.decoders[command]
	if !ok {
		return unsupportedCommand(command)
	}
	return decoder(data, target)
}

func registeredCommandIDs() []uint16 {
	if len(bodyCodecs.commands) == 0 {
		return nil
	}
	return slices.Clone(bodyCodecs.commands)
}

func newBodyCodecRegistry() bodyCodecRegistry {
	entries := bodyCodecEntries()
	encoders := make(map[uint16]bodyEncoder, len(entries))
	decoders := make(map[uint16]bodyDecoder, len(entries))
	commands := make([]uint16, 0, len(entries))
	for _, entry := range entries {
		encoders[entry.command] = entry.encode
		decoders[entry.command] = entry.decode
		commands = append(commands, entry.command)
	}
	return bodyCodecRegistry{encoders: encoders, decoders: decoders, commands: commands}
}

func unsupportedCommand(command uint16) error {
	return oops.In("protocol").Code("unsupported_command").With("command", command).New("unsupported command")
}
