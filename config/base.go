package config

type Config interface {
	Parse(path string) error
}
