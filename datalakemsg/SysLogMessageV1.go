package datalakemsg

//SyslogMessageV1 - structure for Version 1 of messages
type SyslogMessageV1 struct {
	LogMessage
}

// NewSyslogV1 - creates a new Syslog V1 message
func NewSyslogV1() *SyslogMessageV1 {
	m := new(SyslogMessageV1)
	m.Version = "1.0"
	m.Type = "syslogv1"
	return m
}
