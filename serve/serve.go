package serve

import (
	"fmt"
	"net"
	"os"

	"github.com/cloudquery/cq-provider-sdk/plugin/source"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const pluginExecutionMsg = `This binary is a plugin. These are not meant to be executed directly.
Please execute the program that consumes these plugins, which will load any plugins automatically.
Set CQ_PROVIDER_DEBUG=1 to run plugin in debug mode, for additional info see https://docs.cloudquery.io/docs/developers/debugging.
`

type Options struct {
	// Required: Name of provider
	Name string

	// Required: Provider is the actual provider that will be served.
	Plugin *source.SourcePlugin
}

func newCmdServe(opts *Options) *cobra.Command {
	var address string
	var network string
	var logLevel string
	var logFormat string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "serve cloudquery plugin",
		Long:  "serve cloudquery plugin",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			zerologLevel, err := zerolog.ParseLevel(logLevel)
			if err != nil {
				return err
			}
			logger := zerolog.New(os.Stderr).Level(zerologLevel)
			opts.Provider.Logger = logger
			listener, err := net.Listen(network, address)
			if err != nil {
				return fmt.Errorf("failed to listen: %w", err)
			}
			s := grpc.NewServer()
			source.RegisterSourceServer(s, &source.SourceServerImpl{Provider: opts.Provider})
			logger.Info().Str("address", listener.Addr().String()).Msg("server listening")
			if err := s.Serve(listener); err != nil {
				return fmt.Errorf("failed to serve: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&address, "address", "localhost:7777", "address to serve on. can be tcp: `localhost:7777` or unix socket: `/tmp/plugin.rpc.sock`")
	cmd.Flags().StringVar(&network, "network", "tcp", `the network must be "tcp", "tcp4", "tcp6", "unix" or "unixpacket"`)
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level. debug, info, warn, error")
	cmd.Flags().StringVar(&logFormat, "log-format", "text", "log format. text or json")
	return cmd
}

func newCmdRoot(opts *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "cloudquery source plugin",
		Long:  "cloudquery source plugin",
	}
	cmd.AddCommand(newCmdServe(opts))
	return cmd
}

func Serve(opts *Options) {
	if err := newCmdRoot(opts).Execute(); err != nil {
		log.Fatal().Err(err).Msg("plugin failed")
	}
}
