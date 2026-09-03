package event

import "errors"

var ErrBackpressure = errors.New("runtime event backpressure")
