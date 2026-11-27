// Package buildinfo reports which build is running.
//
// Version and Commit are set at link time:
//
//	go build -ldflags "-X github.com/plimsoll-protocol/plimsoll-indexer/internal/buildinfo.Version=v1.2.3 \
//	    -X github.com/plimsoll-protocol/plimsoll-indexer/internal/buildinfo.Commit=$(git rev-parse HEAD)"
//
// A binary built without them falls back to the module version and VCS
// revision that Go embeds, so an operator can still identify the build.
package buildinfo

import (
	"runtime"
	"runtime/debug"
)

// Version and Commit are overridden by -ldflags -X at build time.
var (
	Version = ""
	Commit  = ""
)

// Info describes the running build.
type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Go      string `json:"go"`
}

// Get resolves build information, preferring values injected at link time over
// the ones from debug.ReadBuildInfo.
func Get() Info {
	info := Info{Version: Version, Commit: Commit, Go: runtime.Version()}
	if bi, ok := debug.ReadBuildInfo(); ok {
		if info.Version == "" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			info.Version = bi.Main.Version
		}
		if info.Commit == "" {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" {
					info.Commit = s.Value
					break
				}
			}
		}
	}
	if info.Version == "" {
		info.Version = "devel"
	}
	if info.Commit == "" {
		info.Commit = "unknown"
	}
	return info
}
