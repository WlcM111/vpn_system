package errors

import stderrors "errors"

var (
	ErrNotFound      = stderrors.New("not found")
	ErrInvalidInput  = stderrors.New("invalid input")
	ErrUnauthorized  = stderrors.New("unauthorized")
	ErrForbidden     = stderrors.New("forbidden")
	ErrConflict      = stderrors.New("conflict")
	ErrUnavailable   = stderrors.New("unavailable")
	ErrAlreadyExists = stderrors.New("already exists")
)
