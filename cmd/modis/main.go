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

	// "net/http"
	// _ "net/http/pprof"
	"os"

	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/connection/server"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/storage"
)

// Version information.
var (
	// ReleaseVersion = "0.0.1"
	CommitHash    string
	BuildTS       string
	BranchName    string
	CommitLog     string
	GolangVersion string
)

func main() {
	// go func() {
	// 	http.ListenAndServe(":6060", nil)
	// }()
	sv, configPath := readFlags()
	if sv {
		showVersion()
		os.Exit(0)
	}
	var err error
	_, err = config.LoadConfig(configPath)
	if err != nil {
		fmt.Println("fail to load config", err)
		os.Exit(1)
	}
	cfg := config.DefaultGlobalConfig

	err = log.InitLoggerWithConfig(cfg.Log)
	if err != nil {
		fmt.Println("fail to init logger", err)
		os.Exit(1)
	}

	s, err := storage.Open(&cfg.Storage.ObkvConfig)
	if err != nil {
		fmt.Println("open DB failed")
		log.Fatal("main", "", "open DB failed", log.Errors(err))
		os.Exit(1)
	}

	var tlsConfig *tls.Config
	tlsConfig, err = server.TLSConfig(cfg.Server.SSLCertFile, cfg.Server.SSLKeyFile)
	if err != nil {
		log.Fatal("main", "", "fail to create TLS Config", log.Errors(err))
		os.Exit(1)
	}

	srvCtx := conncontext.NewServerContext(s, &cfg.Server)
	srv := server.NewServer(srvCtx, server.GenClientID())
	if err := srv.ListenAndServe(&cfg.Server, tlsConfig); err != nil {
		log.Warn("main", "", "fail to run server", log.Errors(err))
		return
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
	// fmt.Println("Release Version: ", ReleaseVersion)
	fmt.Println("Git Commit Hash: ", CommitHash)
	fmt.Println("Git Commit Log: ", CommitLog)
	fmt.Println("Git Branch: ", BranchName)
	fmt.Println("UTC Build Time:  ", BuildTS)
	fmt.Println("Golang compiler Version: ", GolangVersion)
}
