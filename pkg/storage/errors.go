package storage

import "errors"

var (
	ErrNotFound               = errors.New("storage: entity not found")
	ErrAlreadyExists          = errors.New("storage: entity already exists")
	ErrInvalidStateTransition = errors.New("storage: invalid state transition attempted")
	ErrInvalidInput           = errors.New("storage: invalid input arguments")
)
