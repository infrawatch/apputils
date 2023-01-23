package system

import (
	"os"
	"os/signal"

	"github.com/infrawatch/apputils/logging"
)

//SpawnSignalHandler spawns goroutine which will wait for given interruption signal(s)
// and in case any is received closes given channel to signal that program should be closed
func SpawnSignalHandler(finish chan bool, logger *logging.Logger, watchedSignals ...os.Signal) {
	interruptChannel := make(chan os.Signal, 1)
	signal.Notify(interruptChannel, watchedSignals...)
	go func() {
	signalLoop:
		for sig := range interruptChannel {
			logger.Metadata(map[string]interface{}{
				"signal": sig,
			})
			logger.Error("Stopping execution on caught signal")
			close(finish)
			break signalLoop
		}
	}()
}
