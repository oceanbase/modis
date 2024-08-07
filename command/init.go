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

var (
	commands map[string]*CmdInfo
)

func init() {
	commands = map[string]*CmdInfo{
		// connections
		"auth":   {Cmd: Auth, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"echo":   {Cmd: Echo, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"ping":   {Cmd: Ping, Arity: -1, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hello":  {Cmd: TempNotSupport, Arity: -1, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"quit":   {Cmd: Quit, Arity: 1, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"select": {Cmd: Select, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"swapdb": {Cmd: SwapDB, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// client
		"client|help": {Cmd: ClientHelp, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"client|info": {Cmd: ClientInfo, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"client|list": {Cmd: ClientList, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// server
		"info":    {Cmd: Info, Arity: -1, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"monitor": {Cmd: Monitor, Arity: 1, Flag: CmdAdmin, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// strings
		"get":         {Cmd: StringCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"set":         {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setnx":       {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setex":       {Cmd: StringCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"psetex":      {Cmd: StringCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"mget":        {Cmd: StringCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"mset":        {Cmd: StringCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"strlen":      {Cmd: StringCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"append":      {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"incr":        {Cmd: StringCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"decr":        {Cmd: StringCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"incrby":      {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"incrbyfloat": {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"decrby":      {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setbit":      {Cmd: StringCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"getbit":      {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"bitcount":    {Cmd: StringCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"getset":      {Cmd: StringCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setrange":    {Cmd: StringCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"getrange":    {Cmd: StringCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// keys
		"type":      {Cmd: ExpireCommon, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"exists":    {Cmd: ExpireCommon, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"del":       {Cmd: ExpireCommon, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"expire":    {Cmd: ExpireCommon, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"expireat":  {Cmd: ExpireCommon, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"pexpire":   {Cmd: ExpireCommon, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"pexpireat": {Cmd: ExpireCommon, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"persist":   {Cmd: ExpireCommon, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"ttl":       {Cmd: ExpireCommon, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"pttl":      {Cmd: ExpireCommon, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// hashes
		"hdel":         {Cmd: HashCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hset":         {Cmd: HashCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hget":         {Cmd: HashCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hgetall":      {Cmd: HashCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hexists":      {Cmd: HashCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hincrby":      {Cmd: HashCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hincrbyfloat": {Cmd: HashCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hkeys":        {Cmd: HashCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hvals":        {Cmd: HashCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hlen":         {Cmd: HashCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hsetnx":       {Cmd: HashCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hmget":        {Cmd: HashCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hmset":        {Cmd: HashCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// sets
		"sadd":        {Cmd: SetCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"smembers":    {Cmd: SetCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"srandmember": {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"scard":       {Cmd: SetCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sismember":   {Cmd: SetCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"spop":        {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"srem":        {Cmd: SetCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sunion":      {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sunionstore": {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sinter":      {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sinterstore": {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sdiff":       {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sdiffstore":  {Cmd: SetCmdWithKey, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"smove":       {Cmd: SetCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// zsets
		"zadd":             {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrange":           {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrevrange":        {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrem":             {Cmd: ZSetCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zcard":            {Cmd: ZSetCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zincrby":          {Cmd: ZSetCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zscore":           {Cmd: ZSetCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrank":            {Cmd: ZSetCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrevrank":         {Cmd: ZSetCmdWithKey, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zremrangebyrank":  {Cmd: ZSetCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zcount":           {Cmd: ZSetCmdWithKey, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrangebyscore":    {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrevrangebyscore": {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zremrangebyscore": {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zunionstore":      {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zinterstore":      {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// list
		"lpush":     {Cmd: ListCmd, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"lpushx":    {Cmd: ListCmd, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"rpush":     {Cmd: ListCmd, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"rpushx":    {Cmd: ListCmd, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"lpop":      {Cmd: ListCmd, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"rpop":      {Cmd: ListCmd, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"lindex":    {Cmd: ListCmd, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"lset":      {Cmd: ListCmd, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"lrange":    {Cmd: ListCmd, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"ltrim":     {Cmd: ListCmd, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"linsert":   {Cmd: ListCmd, Arity: 5, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"llen":      {Cmd: ListCmd, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"lrem":      {Cmd: ListCmd, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"rpoplpush": {Cmd: TempNotSupport, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
	}
}
