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

package log

import (
	"errors"
	"fmt"
	"sync"

	"github.com/oceanbase/modis/config"
	kvlog "github.com/oceanbase/obkv-table-client-go/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	globalMutex         sync.Mutex
	defaultGlobalLogger *kvlog.Logger
)

// wrap kvlog
var InitTraceId = kvlog.InitTraceId

// wrap zap
var (
	Skip        = zap.Skip
	Binary      = zap.Binary
	Bool        = zap.Bool
	Boolp       = zap.Boolp
	ByteString  = zap.ByteString
	Complex128  = zap.Complex128
	Complex128p = zap.Complex128p
	Complex64   = zap.Complex64
	Complex64p  = zap.Complex64p
	Float64     = zap.Float64
	Float64p    = zap.Float64p
	Float32     = zap.Float32
	Float32p    = zap.Float32p
	Int         = zap.Int
	Intp        = zap.Intp
	Int64       = zap.Int64
	Int64p      = zap.Int64p
	Int32       = zap.Int32
	Int32p      = zap.Int32p
	Int16       = zap.Int16
	Int16p      = zap.Int16p
	Int8        = zap.Int8
	Int8p       = zap.Int8p
	String      = zap.String
	Stringp     = zap.Stringp
	Uint        = zap.Uint
	Uintp       = zap.Uintp
	Uint64      = zap.Uint64
	Uint64p     = zap.Uint64p
	Uint32      = zap.Uint32
	Uint32p     = zap.Uint32p
	Uint16      = zap.Uint16
	Uint16p     = zap.Uint16p
	Uint8       = zap.Uint8
	Uint8p      = zap.Uint8p
	Uintptr     = zap.Uintptr
	Uintptrp    = zap.Uintptrp
	Reflect     = zap.Reflect
	Namespace   = zap.Namespace
	Stringer    = zap.Stringer
	Time        = zap.Time
	Timep       = zap.Timep
	Stack       = zap.Stack
	StackSkip   = zap.StackSkip
	Duration    = zap.Duration
	Durationp   = zap.Durationp
	Any         = zap.Any
	Errors      = zap.Error
)

func checkLoggerConfigValidity(cfg config.LogConfig) error {
	if cfg.FilePath == "" {
		return errors.New("should set Log File Name in toml or client config")
	} else if cfg.SingleFileMaxSize <= 0 {
		return errors.New("Single File MaxSize is invalid")
	} else if cfg.MaxAgeFileRem < 0 {
		return errors.New("Max Age File Remain is invalid")
	} else if cfg.MaxBackupFileSize < 0 {
		return errors.New("Max Backup File Size is invalid")
	}
	return nil
}

func InitLoggerWithConfig(cfg config.LogConfig) error {
	fmt.Println("start to init logger with config...")
	err := checkLoggerConfigValidity(cfg)
	if err != nil {
		fmt.Println("fail to check logger config validity, ", err)
		return err
	}
	logFilePath := cfg.FilePath + "/modis.log"
	logWriter := &zapcore.BufferedWriteSyncer{
		WS: zapcore.AddSync(&lumberjack.Logger{
			Filename:   logFilePath,
			MaxSize:    cfg.SingleFileMaxSize,
			MaxBackups: cfg.MaxBackupFileSize,
			MaxAge:     cfg.MaxAgeFileRem,
			Compress:   cfg.Compress,
		}),
		//Size specifies the maximum amount of data the writer will buffered before flushing. Defaults to 256 kB if unspecified.
		Size: kvlog.BufferSize, // async print buffer size
	}
	globalMutex.Lock()
	defaultGlobalLogger = kvlog.NewLogger(logWriter, kvlog.MatchStr2LogLevel(cfg.Level), kvlog.AddCaller())
	globalMutex.Unlock()
	Info("Logger", nil, "init logger with config finished", String("file path", logFilePath))
	fmt.Println("init logger with config finished")
	return nil
}

// Default
func Info(logType string, traceId any, msg string, fields ...kvlog.Field) {
	defaultGlobalLogger.Info(kvlog.AddInfo(logType, traceId, msg), fields...)
}

func Error(logType string, traceId any, msg string, fields ...kvlog.Field) {
	defaultGlobalLogger.Error(kvlog.AddInfo(logType, traceId, msg), fields...)
}

func Warn(logType string, traceId any, msg string, fields ...kvlog.Field) {
	defaultGlobalLogger.Warn(kvlog.AddInfo(logType, traceId, msg), fields...)
}

func DPanic(logType string, traceId any, msg string, fields ...kvlog.Field) {
	defaultGlobalLogger.DPanic(kvlog.AddInfo(logType, traceId, msg), fields...)
}

func Panic(logType string, traceId any, msg string, fields ...kvlog.Field) {
	defaultGlobalLogger.Panic(kvlog.AddInfo(logType, traceId, msg), fields...)
}

func Fatal(logType string, traceId any, msg string, fields ...kvlog.Field) {
	defaultGlobalLogger.Fatal(kvlog.AddInfo(logType, traceId, msg), fields...)
}

func Debug(logType string, traceId any, msg string, fields ...kvlog.Field) {
	defaultGlobalLogger.Debug(kvlog.AddInfo(logType, traceId, msg), fields...)
}
