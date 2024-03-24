package remote

import "errors"

var (
	ErrInvalidURI   = errors.New("invalid URI")
	ErrDoesNotExist = errors.New("object does not exist")
)
