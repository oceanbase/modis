/*-
 * #%L
 * Modis
 * %%
 * Copyright (C) 2024 OceanBase
 * %%
 * Modis is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
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
