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

package conncontext

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/oceanbase/modis/config"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/metrics"
	"github.com/oceanbase/modis/storage"
	"github.com/oceanbase/modis/util"
	cmap "github.com/orcaman/concurrent-map/v2"
)

const (
	// DefaultNamespace is default namespace of DB
	DefaultNamespace = "default"
)

type SupervisedMode int
type ClientID int64

func (id ClientID) String() string {
	return fmt.Sprintf("%v", int(id))
}

const (
	SupervisedNone    SupervisedMode = iota // 0
	SupervisedUnknown                       // 1
	SupervisedSystemd                       // 2
	SupervisedUpstart                       // 3
)

// ServerContext connect server and clients
type ServerContext struct {
	Storage         storage.Storage
	StartTime       time.Time
	Password        string
	DbNum           int64
	dbs             []*storage.DB
	SuperMode       SupervisedMode
	RunID           string
	Port            int
	ModisPath       string
	ConfigPath      string
	ClientNum       int
	MaxClientNum    int
	TotalClientNum  int64
	RejectClientNum int64
	Backend         string
	// [cliend id, CodecContext], record all clients
	Clients cmap.ConcurrentMap[ClientID, *CodecContext]
	// [cliend id, CodecContext], record clients with monitor
	Monitors cmap.ConcurrentMap[ClientID, *CodecContext]

	// atomic, include all clients
	TotalCmdNum     *metrics.Metrics
	TotalReadBytes  *metrics.Metrics
	TotalWriteBytes *metrics.Metrics

	ClientsPeakMemInput  int64
	ClientsPeakMemOutput int64
}

// NewServerContext creates a new client context
func NewServerContext(s storage.Storage, cfg *config.Config, cfgPath string) (*ServerContext, error) {
	servCfg := &cfg.Server
	sc := &ServerContext{
		Storage:         s,
		Password:        servCfg.Password,
		DbNum:           servCfg.DBNum,
		dbs:             make([]*storage.DB, 0, servCfg.DBNum),
		ClientNum:       0,
		MaxClientNum:    servCfg.MaxConnection,
		TotalClientNum:  0,
		RejectClientNum: 0,
		TotalCmdNum:     metrics.NewMetrics(),
		TotalReadBytes:  metrics.NewMetrics(),
		TotalWriteBytes: metrics.NewMetrics(),
		Backend:         cfg.Storage.Backend,
		Clients:         cmap.NewStringer[ClientID, *CodecContext](),
		Monitors:        cmap.NewStringer[ClientID, *CodecContext](),
	}

	// init modis path
	err := sc.initModisPath()
	if err != nil {
		return nil, err
	}

	// init path
	sc.ConfigPath, err = filepath.Abs(cfgPath)
	if err != nil {
		log.Warn("server", nil, "fail to init config path", log.Errors(err), log.String("config path", cfgPath))
		return nil, err
	}

	// init port
	var addr *net.TCPAddr
	addr, err = net.ResolveTCPAddr("tcp", servCfg.Listen)
	if err != nil {
		log.Warn("server", nil, "fail to resolve tcp addr", log.Errors(err))
		return nil, err
	}
	sc.Port = addr.Port

	// init run_id
	rb, err := util.GenRandomBytes(runIDLength)
	if err != nil {
		log.Warn("server", nil, "fail to init run_id", log.Errors(err))
		return nil, err
	}
	sc.RunID = hex.EncodeToString(rb)

	// init db
	for i := int64(0); i < servCfg.DBNum; i++ {
		sc.dbs = append(sc.dbs, storage.NewDB(DefaultNamespace, int64(i), s))
	}

	// init supervised mode
	err = sc.initSupervised(servCfg)
	if err != nil {
		return nil, err
	}
	return sc, nil
}

// GetDB prevents visit db out of range
func (sc *ServerContext) GetDB(index int64) (*storage.DB, error) {
	if index >= sc.DbNum {
		return nil, errors.New("visit db out of range")
	}
	sc.dbs[index].IsInit = true
	return sc.dbs[index], nil
}

func (sc *ServerContext) IsDBInit(index int64) bool {
	if index >= sc.DbNum {
		return false
	}
	return sc.dbs[index].IsInit
}

// initModisPath fetch modis real path
func (sc *ServerContext) initModisPath() error {
	execPath, err := os.Executable()
	if err != nil {
		log.Warn("server", nil, "fail to get executable path", log.Errors(err))
		return err
	}
	sc.ModisPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		log.Warn("server", nil, "fail to get real path of executable", log.Errors(err))
		return err
	}
	return nil
}

// initSupervised init supervised mode
func (sc *ServerContext) initSupervised(cfg *config.ServerConfig) error {
	switch strings.ToLower(cfg.Supervised) {
	case "auto":
		if _, exist := os.LookupEnv("UPSTART_JOB"); exist {
			sc.SuperMode = SupervisedUpstart
		} else if _, exist := os.LookupEnv("NOTIFY_SOCKET"); exist {
			sc.SuperMode = SupervisedSystemd
		} else {
			sc.SuperMode = SupervisedNone
		}
	case "systemd":
		sc.SuperMode = SupervisedSystemd
	case "upstart":
		sc.SuperMode = SupervisedUpstart
	default:
		sc.SuperMode = SupervisedNone
	}
	if sc.SuperMode == SupervisedSystemd {
		err := util.SdNotify("STATUS=Modis is loading...\n")
		if err != nil {
			return err
		}
	} else if sc.SuperMode == SupervisedUpstart {
		job := os.Getenv("UPSTART_JOB")
		if job == "" {
			err := errors.New("UPSTART_JOB is empty")
			log.Warn("server", nil, "fail to init supervised mode", log.Errors(err))
			return err
		}
		err := syscall.Kill(os.Getpid(), syscall.SIGSTOP)
		if err != nil {
			log.Warn("server", nil, "fail to raise SIGSTOP", log.Errors(err))
			return err
		}
		err = os.Unsetenv("UPSTART_JOB")
		if err != nil {
			log.Warn("server", nil, "fail to unset UPSTART_JOB", log.Errors(err))
			return err
		}
	}
	return nil
}

func (sc *ServerContext) StartMetricsTicker() {
	go func() {
		for range time.Tick(1 * time.Second) {
			sc.TotalCmdNum.Observe()
			sc.TotalReadBytes.Observe()
			sc.TotalWriteBytes.Observe()

			threshold := 10
			if sc.Clients.Count() > threshold {
				threshold = sc.Clients.Count() / threshold
			}
			var peekInput int64 = 0
			for i, cliCtx := range sc.Clients.Items() {
				if int64(i) >= int64(threshold) {
					break
				}
				input := int64(cliCtx.Reader.Size()) + cliCtx.TotalArgvLen
				if input > peekInput {
					peekInput = input
				}
			}
			if peekInput > sc.ClientsPeakMemInput {
				sc.ClientsPeakMemInput = peekInput
			}
		}
	}()
}
