package domain

const (
	ProtocolHeaderSize            = 8
	FrameHeaderSize               = 8
	FrameTypeAMQP            byte = 0
	FrameTypeSASL            byte = 1
	DescriptorOpen           byte = 0x10
	DescriptorBegin          byte = 0x11
	DescriptorAttach         byte = 0x12
	DescriptorFlow           byte = 0x13
	DescriptorTransfer       byte = 0x14
	DescriptorDisposition    byte = 0x15
	DescriptorDetach         byte = 0x16
	DescriptorEnd            byte = 0x17
	DescriptorClose          byte = 0x18
	DescriptorError          byte = 0x1d
	DescriptorSASLMechanisms byte = 0x40
	DescriptorSASLInit       byte = 0x41
	DescriptorSASLChallenge  byte = 0x42
	DescriptorSASLResponse   byte = 0x43
	DescriptorSASLOutcome    byte = 0x44
)

var Names = map[byte]string{DescriptorOpen: "open", DescriptorBegin: "begin", DescriptorAttach: "attach", DescriptorFlow: "flow", DescriptorTransfer: "transfer", DescriptorDisposition: "disposition", DescriptorDetach: "detach", DescriptorEnd: "end", DescriptorClose: "close", DescriptorSASLMechanisms: "sasl-mechanisms", DescriptorSASLInit: "sasl-init", DescriptorSASLChallenge: "sasl-challenge", DescriptorSASLResponse: "sasl-response", DescriptorSASLOutcome: "sasl-outcome"}
