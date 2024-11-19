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

package command

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/oceanbase/modis/connection/conncontext"
	"github.com/oceanbase/modis/log"
	"github.com/oceanbase/modis/protocol/resp"
	"github.com/oceanbase/modis/util"
)

var (
	GitSha1       string
	GitDirty      string
	BuildID       string
	CommitID      string
	ModisVer      string
	supervisedMap = map[conncontext.SupervisedMode]string{
		conncontext.SupervisedNone:    "none",
		conncontext.SupervisedSystemd: "systemd",
		conncontext.SupervisedUpstart: "upstart",
		conncontext.SupervisedUnknown: "unknown",
	}
)

const (
	modisMode = "standalone"
)

type DBInfo struct {
	Keys    int64
	Expires int64
}

// Info print modis info
func Info(ctx *CmdContext) error {
	type void struct{}
	var val void
	sections := make(map[string]void)
	argc := len(ctx.Args)
	useDefault := false
	if argc == 0 {
		useDefault = true
	} else {
		for _, argv := range ctx.Args {
			curArg := strings.ToLower(util.BytesToString(argv))
			if strings.EqualFold("default", curArg) {
				useDefault = true
				break
			}
			sections[curArg] = val
		}
	}
	if useDefault {
		sections = map[string]void{
			"server":      {},
			"clients":     {},
			"memory":      {},
			"persistence": {},
			"stats":       {},
			"replication": {},
			"cpu":         {},
			"module_list": {},
			"errorstats":  {},
			"cluster":     {},
			"keyspace":    {},
		}
	}

	var infoBuilder strings.Builder
	var err error
	curTime := time.Now().Unix()
	idx := -1
	for section := range sections {
		switch section {
		case "server":
			if idx++; idx > 0 {
				if _, err = infoBuilder.WriteString("\r\n"); err != nil {
					break
				}
			}
			upTime := curTime - ctx.ServCtx.StartTime.Unix()
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# Server\r\n"+
					"modis_version:%s\r\n"+
					"modis_git_sha1:%s\r\n"+
					"modis_git_dirty:%s\r\n"+
					"modis_build_id:%s\r\n"+
					"modis_mode:%s\r\n"+
					"process_id:%d\r\n"+
					"process_supervised:%s\r\n"+
					"run_id:%s\r\n"+
					"tcp_port:%d\r\n"+
					"uptime_in_seconds:%d\r\n"+
					"executable:%s\r\n"+
					"config_file:%s\r\n",
				ModisVer,
				GitSha1,
				GitDirty,
				BuildID,
				modisMode,
				os.Getpid(),
				supervisedMap[ctx.ServCtx.SuperMode],
				ctx.ServCtx.RunID,
				ctx.ServCtx.Port,
				upTime,
				ctx.ServCtx.ModisPath,
				ctx.ServCtx.ConfigPath,
			))
		case "clients":
			if idx++; idx > 0 {
				if _, err = infoBuilder.WriteString("\r\n"); err != nil {
					break
				}
			}
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# Clients\r\n"+
					"connected_clients:%d\r\n"+
					"maxclients:%d\r\n"+
					"client_recent_max_input_buffer:%d\r\n"+
					"client_recent_max_output_buffer:%d\r\n",
				ctx.ServCtx.ClientNum.Load(),
				ctx.ServCtx.MaxClientNum,
				ctx.ServCtx.ClientsPeakMemInput,
				ctx.ServCtx.ClientsPeakMemOutput,
			))
		case "persistence":
			if idx++; idx > 0 {
				if _, err = infoBuilder.WriteString("\r\n"); err != nil {
					break
				}
			}
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# Persistence\r\n"+
					"backend:%s\r\n",
				ctx.ServCtx.Backend,
			))
		case "stats":
			if idx++; idx > 0 {
				if _, err = infoBuilder.WriteString("\r\n"); err != nil {
					break
				}
			}
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# Stats\r\n"+
					"total_connections_received:%d\r\n"+
					"total_commands_processed:%d\r\n"+
					"instantaneous_ops_per_sec:%.2f\r\n"+
					"total_net_input_bytes:%d\r\n"+
					"total_net_output_bytes:%d\r\n"+
					"instantaneous_input_kbps:%.2f\r\n"+
					"instantaneous_output_kbps:%.2f\r\n"+
					"rejected_connections:%d\r\n",
				ctx.ServCtx.TotalClientNum,
				ctx.ServCtx.TotalCmdNum.GetSample(),
				ctx.ServCtx.TotalCmdNum.GetAvg(),
				ctx.ServCtx.TotalReadBytes.GetSample(),
				ctx.ServCtx.TotalWriteBytes.GetSample(),
				ctx.ServCtx.TotalReadBytes.GetAvg(),
				ctx.ServCtx.TotalWriteBytes.GetAvg(),
				ctx.ServCtx.RejectClientNum,
			))
		case "cpu":
			if idx++; idx > 0 {
				if _, err = infoBuilder.WriteString("\r\n"); err != nil {
					break
				}
			}
			self_rusage := new(syscall.Rusage)
			child_rusage := new(syscall.Rusage)
			err = syscall.Getrusage(syscall.RUSAGE_SELF, self_rusage)
			if err != nil {
				log.Warn("command", ctx.TraceID, "fail to get RUSAGE_SELF", log.Errors(err))
				break
			}
			err = syscall.Getrusage(syscall.RUSAGE_CHILDREN, child_rusage)
			if err != nil {
				log.Warn("command", ctx.TraceID, "fail to get RUSAGE_CHILDREN", log.Errors(err))
				break
			}
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# CPU\r\n"+
					"used_cpu_sys:%d.%06d\r\n"+
					"used_cpu_user:%d.%06d\r\n"+
					"used_cpu_sys_children:%d.%06d\r\n"+
					"used_cpu_user_children:%d.%06d\r\n"+
					"\r\n",
				self_rusage.Stime.Sec, self_rusage.Stime.Usec,
				self_rusage.Utime.Sec, self_rusage.Utime.Usec,
				child_rusage.Stime.Sec, child_rusage.Stime.Usec,
				child_rusage.Utime.Sec, child_rusage.Utime.Usec,
			))
		case "commandstats":
			if idx++; idx > 0 {
				if _, err = infoBuilder.WriteString("\r\n"); err != nil {
					break
				}
			}
			_, err = infoBuilder.WriteString("# Commandstats\r\n")
			if err != nil {
				log.Warn("command", ctx.TraceID, "fail to write string to infoBuilder", log.Errors(err))
				break
			}
			for cmdName, v := range commands {
				if v.Stats.Calls > 0 {
					_, err = infoBuilder.WriteString(fmt.Sprintf(
						"cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f\r\n"+
							"\r\n",
						cmdName, v.Stats.Calls, v.Stats.MicroSec, v.Stats.GetUsecPerCall(),
					))
				}
			}
		case "cluster":
			if idx++; idx > 0 {
				if _, err = infoBuilder.WriteString("\r\n"); err != nil {
					break
				}
			}
			_, err = infoBuilder.WriteString(
				"# Cluster\r\n" +
					"cluster_enabled:0\r\n", // no cluster currenctly
			)
			// TODO: keyspace
			// case "keyspace":
			// 	if idx++; idx > 0 {
			// 		if _, err = infoBuilder.WriteString("\r\n"); err != nil {
			// 			break
			// 		}
			// 	}
			// 	_, err = infoBuilder.WriteString("# Keyspace\r\n")
			// 	if err != nil {
			// 		log.Warn("command", ctx.TraceID, "fail to write string to infoBuilder", log.Errors(err))
			// 		break
			// 	}
			// 	err = formatDBInfo(ctx, &infoBuilder)
			// 	if err != nil {
			// 		log.Warn("command", ctx.TraceID, "fail to format db info", log.Errors(err))
			// 		break
			// 	}
		}

		if err != nil {
			break
		}
	}
	if err != nil {
		ctx.OutContent = resp.EncError("ERR fetch info error, " + err.Error())
	} else {
		ctx.OutContent = resp.EncBulkString(infoBuilder.String())
	}
	return nil
}

func Monitor(ctx *CmdContext) error {
	ctx.ServCtx.Monitors.Set(ctx.CodecCtx.ID, ctx.CodecCtx)
	ctx.CodecCtx.Flag |= conncontext.ClientMonitor
	ctx.OutContent = resp.ResponsesOk
	return nil
}
