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
	tables   []string
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
		"get":         {Cmd: Get, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"set":         {Cmd: Set, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setnx":       {Cmd: SetNx, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setex":       {Cmd: SetEx, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"psetex":      {Cmd: PSetEx, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"mget":        {Cmd: MGet, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"mset":        {Cmd: MSet, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"strlen":      {Cmd: Strlen, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"append":      {Cmd: Append, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"incr":        {Cmd: Incr, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"decr":        {Cmd: Decr, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"incrby":      {Cmd: IncrBy, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"incrbyfloat": {Cmd: IncrByFloat, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"decrby":      {Cmd: DecrBy, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setbit":      {Cmd: SetBit, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"getbit":      {Cmd: GetBit, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"bitcount":    {Cmd: BitCount, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"getset":      {Cmd: GetSet, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"setrange":    {Cmd: SetRange, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"getrange":    {Cmd: GetRange, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// keys
		"type":      {Cmd: Type, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"exists":    {Cmd: Exists, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"del":       {Cmd: Delete, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"expire":    {Cmd: Expire, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"expireat":  {Cmd: ExpireAt, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"pexpire":   {Cmd: PExpire, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"pexpireat": {Cmd: PExpireAt, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"persist":   {Cmd: Persist, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"ttl":       {Cmd: TTL, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"pttl":      {Cmd: PTTL, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// hashes
		"hdel":         {Cmd: HDel, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hset":         {Cmd: HashCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hget":         {Cmd: HGet, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hgetall":      {Cmd: HGetAll, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hexists":      {Cmd: HExists, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hincrby":      {Cmd: HIncrBy, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hincrbyfloat": {Cmd: HIncrByFloat, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hkeys":        {Cmd: HKeys, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hvals":        {Cmd: HVals, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hlen":         {Cmd: HLen, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hsetnx":       {Cmd: HSetNX, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hmget":        {Cmd: HMGet, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"hmset":        {Cmd: HashCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// sets
		"sadd":        {Cmd: SAdd, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"smembers":    {Cmd: SMembers, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"srandmember": {Cmd: SRandMember, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"scard":       {Cmd: SCard, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sismember":   {Cmd: SIsmember, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"spop":        {Cmd: SPop, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"srem":        {Cmd: SRem, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sunion":      {Cmd: SUnion, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sunionstore": {Cmd: SUnionStore, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sinter":      {Cmd: SInter, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sinterstore": {Cmd: SInterStore, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sdiff":       {Cmd: SDiffServer, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"sdiffstore":  {Cmd: SDiffStore, Arity: -2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"smove":       {Cmd: SMove, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},

		// zsets
		"zadd":             {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrange":           {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrevrange":        {Cmd: ZSetCmdWithKey, Arity: -4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrem":             {Cmd: ZSetCmdWithKey, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zcard":            {Cmd: ZSetCmdWithKey, Arity: 2, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zincrby":          {Cmd: ZIncrBy, Arity: 4, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zscore":           {Cmd: ZSetCmdWithKeyMember, Arity: 3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrank":            {Cmd: ZSetCmdWithKeyMember, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
		"zrevrank":         {Cmd: ZSetCmdWithKeyMember, Arity: -3, Flag: CmdNone, Stats: CmdStats{Calls: 0, MicroSec: 0}},
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

	tables = []string{
		"modis_string_table",
		"modis_hash_table",
		"modis_set_table",
		"modis_list_table",
		"modis_zset_table",
	}
}
