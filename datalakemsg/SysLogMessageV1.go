package datalakemsg

//SyslogMessageV1 - structure for Version 1 of messages
type SyslogMessageV1 struct {
	logV1
}

// NewSyslogV1 - creates a new Syslog V1 message
func NewSyslogV1(id, timestamp, body string) *SyslogMessageV1 {
	m := new(SyslogMessageV1)
	m.Version = "1.0"
	m.Type = "syslogv1"
	m.Body = body
	m.ID = id
	m.TimeStamp = timestamp
	return m
}
