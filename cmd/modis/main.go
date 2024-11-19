/*
 * Copyright (c) 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"crypto/tls"
	"flag"
	"fmt"

	"github.com/fsnotify/fsnotify"

	// "net/http"
	// _ "net/http/pprof"
	"os"

	"github.com/oceanbase/modis/command"
	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/connection/server"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/storage"
)

// Version information.
var (
	GolangVersion string
)

func main() {
	// try read flags
	sv, configPath := readFlags()
	if sv {
		showVersion()
		os.Exit(0)
	}

	// init config
	var err error
	_, err = config.LoadConfig(configPath)
	if err != nil {
		fmt.Println("fail to load config, ", err)
		os.Exit(1)
	}

	log_watcher, err := fsnotify.NewWatcher()
	if err != nil {
		fmt.Println("fail to init watcher, ", err)
		os.Exit(1)
	}
	defer log_watcher.Close()
	cfg := config.DefaultGlobalConfig

	err = log.InitLoggerWithConfig(cfg.Log, log_watcher)
	if err != nil {
		fmt.Println("fail to init logger, ", err)
		os.Exit(1)
	}
	defer log.Sync()

	// init storage
	s, err := storage.Open(&cfg.Storage.ObkvConfig)
	if err != nil {
		fmt.Println("open DB failed", err.Error())
		log.Fatal("main", "", "open DB failed", log.Errors(err))
		os.Exit(1)
	}

	// init TLS
	var tlsConfig *tls.Config
	tlsConfig, err = server.TLSConfig(cfg.Server.SSLCertFile, cfg.Server.SSLKeyFile)
	if err != nil {
		log.Fatal("main", "", "fail to create TLS Config", log.Errors(err))
		os.Exit(1)
	}

	// init server
	srvCtx, err := conncontext.NewServerContext(s, &cfg, configPath)
	if err != nil {
		log.Warn("main", "", "fail new server context", log.Errors(err))
		os.Exit(1)
	}
	srv := server.NewServer(srvCtx, server.GenClientID())
	if err := srv.ListenAndServe(&cfg.Server, tlsConfig); err != nil {
		log.Warn("main", "", "fail to run server", log.Errors(err))
		os.Exit(1)
	}
}

func readFlags() (showVersion bool, config string) {
	flag.BoolVar(&showVersion, "v", false, "Show Version")
	flag.StringVar(&config, "c", "", "Assignment config")
	flag.Parse()
	return
}

// ShowVersion print version info about modis
func showVersion() {
	fmt.Println("Welcome to modis.")
	fmt.Println("Modis Version:", command.ModisVer)
	fmt.Println("Commit ID:", command.CommitID)
	fmt.Println("Golang compiler Version:", GolangVersion)
}
