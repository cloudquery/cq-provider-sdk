package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cloudquery/cq-provider-sdk/stats"
	"github.com/hashicorp/go-plugin"
)

// ReattachConfig holds the information needed to be able to attach
// itself to a provider process, so it can drive the process.
type ReattachConfig struct {
	Protocol string
	Pid      int
	Test     bool
	Addr     ReattachConfigAddr
}

// ReattachConfigAddr is a JSON-encoding friendly version of net.Addr.
type ReattachConfigAddr struct {
	Network string
	String  string
}

// DebugServe starts a plugin server in debug mode; this should only be used
// when the provider will manage its own lifecycle. It is not recommended for
// normal usage; Serve is the correct function for that.
func DebugServe(ctx context.Context, opts *Options) (ReattachConfig, <-chan struct{}, error) {
	reattachCh := make(chan *plugin.ReattachConfig)
	closeCh := make(chan struct{})

	opts.TestConfig = &plugin.ServeTestConfig{
		Context:          ctx,
		ReattachConfigCh: reattachCh,
		CloseCh:          closeCh,
	}

	stats.Start(ctx, &stats.Options{Logger: opts.Logger})
	go serve(opts)

	var config *plugin.ReattachConfig
	select {
	case config = <-reattachCh:
	case <-time.After(2 * time.Second):
		return ReattachConfig{}, closeCh, errors.New("timeout waiting on reattach config")
	}

	if config == nil {
		return ReattachConfig{}, closeCh, errors.New("nil reattach config received")
	}

	return ReattachConfig{
		Protocol: string(config.Protocol),
		Pid:      config.Pid,
		Test:     config.Test,
		Addr: ReattachConfigAddr{
			Network: config.Addr.Network(),
			String:  config.Addr.String(),
		},
	}, closeCh, nil
}

// Debug starts a debug server and controls its lifecycle, printing the
// information needed for CloudQuery to connect to the provider via stdout.
// os.Interrupt will be captured and used to stop the server.
func Debug(ctx context.Context, providerName string, opts *Options) error {
	ctx, cancel := context.WithCancel(ctx)
	// Ctrl-C will stop the server
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer func() {
		signal.Stop(sigCh)
		cancel()
	}()
	config, closeCh, err := DebugServe(ctx, opts)
	if err != nil {
		return fmt.Errorf("error launching debug server: %w", err)
	}
	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	reattachBytes, err := json.Marshal(map[string]ReattachConfig{
		providerName: config,
	})
	if err != nil {
		return fmt.Errorf("error building reattach string: %w", err)
	}

	wd, _ := os.Getwd()
	reattachPath := filepath.Join(wd, ".cq_reattach")
	if err := saveProviderReattach(reattachPath, reattachBytes); err != nil {
		return fmt.Errorf("error failed saving reattach config: %w", err)
	}

	fmt.Printf("Provider started, to attach Cloudquery set the CQ_REATTACH_PROVIDERS env var:\n\n")
	switch runtime.GOOS {
	case "windows":
		fmt.Printf("\tCommand Prompt:\tset CQ_REATTACH_PROVIDERS=\"%s\"\n", reattachPath)
		fmt.Printf("\tPowerShell:\t$env:CQ_REATTACH_PROVIDERS=\"%s\"\n", reattachPath)
	case "linux", "darwin":
		fmt.Printf("\texport CQ_REATTACH_PROVIDERS=%s\n", reattachPath)
	default:
		fmt.Println(string(reattachBytes))
	}
	fmt.Println("")

	// wait for the server to be done
	<-closeCh
	return nil
}
