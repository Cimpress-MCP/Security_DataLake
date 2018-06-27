package dispatch

type Payload interface {
	Process() error
}
