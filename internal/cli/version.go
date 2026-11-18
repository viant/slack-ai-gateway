package cli

var version = "dev"

// SetVersion initializes the CLI version before executing commands.
func SetVersion(value string) {
	if value != "" {
		version = value
	}
}
