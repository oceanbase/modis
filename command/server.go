/*-
 * #%L
 * Modis
 * %%
 * Copyright (C) 2021 OceanBase
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
	"github.com/oceanbase/modis/storage/obkv"
	"github.com/oceanbase/modis/util"
)

var (
	GitSha1       string
	GitDirty      string
	BuildID       string
	supervisedMap = map[conncontext.SupervisedMode]string{
		conncontext.SupervisedNone:    "none",
		conncontext.SupervisedSystemd: "systemd",
		conncontext.SupervisedUpstart: "upstart",
		conncontext.SupervisedUnknown: "unknown",
	}
)

const (
	ModisVer  = "0.1.0"
	modisMode = "standalone"
)

type DBInfo struct {
	Keys    int64
	Expires int64
}

func getDBInfo(ctx *CmdContext, db int64) (*DBInfo, error) {
	var tbInfo *obkv.TableInfo
	var err error
	dbInfo := &DBInfo{Keys: 0, Expires: 0}
	for _, tbName := range tables {
		tbInfo, err = ctx.CodecCtx.DB.Storage.GetTableInfo(ctx.CodecCtx.DB.Ctx, db, tbName)
		if err != nil {
			log.Warn("command", ctx.TraceID, "fail to get table info",
				log.Errors(err), log.Int64("db", db), log.String("table name", tbName))
			return nil, err
		}
		dbInfo.Keys += tbInfo.Keys
		dbInfo.Expires += tbInfo.Expires
	}
	return dbInfo, nil
}

func formatDBInfo(ctx *CmdContext, infoBuilder *strings.Builder) error {
	var dbInfo *DBInfo
	var err error
	for db := int64(0); db < ctx.ServCtx.DbNum; db++ {
		if !ctx.ServCtx.IsDBInit(db) {
			continue
		}
		dbInfo, err = getDBInfo(ctx, db)
		if err != nil {
			log.Warn("command", ctx.TraceID, "fail to get db info",
				log.Errors(err), log.Int64("db", db))
			return err
		}
		if dbInfo.Keys > 0 {
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"db%d:keys=%d,expires=%d\r\n",
				db, dbInfo.Keys, dbInfo.Expires,
			))
			if err != nil {
				log.Warn("command", ctx.TraceID, "fail to write string builder",
					log.Errors(err), log.Int64("db", db))
				return err
			}
		}
	}
	return nil
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
			if curArg == "default" {
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
	for section := range sections {
		switch section {
		case "server":
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
					"config_file:%s\r\n"+
					"\r\n",
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
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# Clients\r\n"+
					"connected_clients:%d\r\n"+
					"maxclients:%d\r\n"+
					"client_recent_max_input_buffer:%d\r\n"+
					"client_recent_max_output_buffer:%d\r\n"+
					"\r\n",
				ctx.ServCtx.ClientNum,
				ctx.ServCtx.MaxClientNum,
				ctx.ServCtx.ClientsPeakMemInput,
				ctx.ServCtx.ClientsPeakMemOutput,
			))
		case "persistence":
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# Persistence\r\n"+
					"backend:%s\r\n"+
					"\r\n",
				ctx.ServCtx.Backend,
			))
		case "stats":
			_, err = infoBuilder.WriteString(fmt.Sprintf(
				"# Stats\r\n"+
					"total_connections_received:%d\r\n"+
					"total_commands_processed:%d\r\n"+
					"instantaneous_ops_per_sec:%.2f\r\n"+
					"total_net_input_bytes:%d\r\n"+
					"total_net_output_bytes:%d\r\n"+
					"instantaneous_input_kbps:%.2f\r\n"+
					"instantaneous_output_kbps:%.2f\r\n"+
					"rejected_connections:%d\r\n"+
					"\r\n",
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
					"used_cpu_sys:%d.%06d\r\n"+
					"used_cpu_user:%d.%06d\r\n"+
					"\r\n",
				self_rusage.Stime.Sec, self_rusage.Stime.Usec,
				self_rusage.Utime.Sec, self_rusage.Utime.Usec,
				child_rusage.Stime.Sec, child_rusage.Stime.Usec,
				child_rusage.Utime.Sec, child_rusage.Utime.Usec,
			))
		case "commandstats":
			_, err = infoBuilder.WriteString("# Commandstats\r\n")
			if err != nil {
				log.Warn("command", ctx.TraceID, "fail to write string to infoBuilder", log.Errors(err))
				break
			}
			for cmdName, v := range commands {
				_, err = infoBuilder.WriteString(fmt.Sprintf(
					"cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f\r\n"+
						"\r\n",
					cmdName, v.Stats.Calls, v.Stats.MicroSec, v.Stats.GetUsecPerCall(),
				))
			}
		case "cluster":
			_, err = infoBuilder.WriteString(
				"# Cluster\r\n" +
					"cluster_enabled:0\r\n" + // no cluster currenctly
					"\r\n",
			)
			// TODO: keyspace
			// case "keyspace":
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
			// 	_, err = infoBuilder.WriteString("\r\n")
			// 	if err != nil {
			// 		log.Warn("command", ctx.TraceID, "fail to write string to infoBuilder", log.Errors(err))
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
