package datalakemsg

//LogMesasge - Generic structure of our messages
type logV1 struct {
	base
	ID        string `json:"id,omitempty"`
	TimeStamp string `json:"timestamp,omitempty"`
	Body      string `json:"body"`
}
