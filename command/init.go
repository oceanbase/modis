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

var commands map[string]CmdInfo

func init() {
	commands = map[string]CmdInfo{
		// connections
		"auth":   {Cmd: Auth, Arity: 2},
		"echo":   {Cmd: Echo, Arity: 2},
		"ping":   {Cmd: Ping, Arity: -1},
		"hello":  {Cmd: TempNotSupport, Arity: -1},
		"quit":   {Cmd: Quit, Arity: 1},
		"select": {Cmd: Select, Arity: 2},
		"swapdb": {Cmd: SwapDB, Arity: 3},

		// strings
		"get":         {Cmd: Get, Arity: 2},
		"set":         {Cmd: Set, Arity: -3},
		"setnx":       {Cmd: SetNx, Arity: 3},
		"setex":       {Cmd: SetEx, Arity: 4},
		"psetex":      {Cmd: PSetEx, Arity: 4},
		"mget":        {Cmd: MGet, Arity: -2},
		"mset":        {Cmd: MSet, Arity: -3},
		"strlen":      {Cmd: Strlen, Arity: 2},
		"append":      {Cmd: Append, Arity: 3},
		"incr":        {Cmd: TempNotSupport, Arity: 2},
		"decr":        {Cmd: TempNotSupport, Arity: 2},
		"incrby":      {Cmd: TempNotSupport, Arity: 3},
		"incrbyfloat": {Cmd: TempNotSupport, Arity: 3},
		"decrby":      {Cmd: TempNotSupport, Arity: 3},
		"setbit":      {Cmd: SetBit, Arity: 4},
		"getbit":      {Cmd: GetBit, Arity: 3},
		"bitcount":    {Cmd: BitCount, Arity: -2},
		"getset":      {Cmd: GetSet, Arity: 3},
		"setrange":    {Cmd: SetRange, Arity: 4},
		"getrange":    {Cmd: GetRange, Arity: 4},

		// keys
		"type":      {Cmd: Type, Arity: 2},
		"exists":    {Cmd: TempNotSupport, Arity: -2},
		"del":       {Cmd: Delete, Arity: -2},
		"expire":    {Cmd: TempNotSupport, Arity: 3},
		"expireat":  {Cmd: TempNotSupport, Arity: 3},
		"pexpire":   {Cmd: TempNotSupport, Arity: 3},
		"pexpireat": {Cmd: TempNotSupport, Arity: 3},
		"persist":   {Cmd: TempNotSupport, Arity: 2},
		"ttl":       {Cmd: TempNotSupport, Arity: 2},
		"pttl":      {Cmd: TempNotSupport, Arity: 2},

		// hashes
		"hdel":         {Cmd: HDel, Arity: -3},
		"hset":         {Cmd: TempNotSupport, Arity: -4},
		"hget":         {Cmd: HGet, Arity: 3},
		"hgetall":      {Cmd: HGetAll, Arity: 2},
		"hexists":      {Cmd: HExists, Arity: 3},
		"hincrby":      {Cmd: HIncrBy, Arity: 4},
		"hincrbyfloat": {Cmd: HIncrByFloat, Arity: 4},
		"hkeys":        {Cmd: HKeys, Arity: 2},
		"hvals":        {Cmd: HVals, Arity: 2},
		"hlen":         {Cmd: HLen, Arity: 2},
		"hsetnx":       {Cmd: HSetNX, Arity: 4},
		"hmget":        {Cmd: HMGet, Arity: -3},
		"hmset":        {Cmd: TempNotSupport, Arity: -3},

		// sets
		"sadd":        {Cmd: SAdd, Arity: -3},
		"smembers":    {Cmd: TempNotSupport, Arity: 2},
		"srandmember": {Cmd: SRandMember, Arity: 3},
		"scard":       {Cmd: TempNotSupport, Arity: 2},
		"sismember":   {Cmd: SIsmember, Arity: 3},
		"spop":        {Cmd: SPop, Arity: -2},
		"srem":        {Cmd: SRem, Arity: -3},
		"sunion":      {Cmd: SUnion, Arity: -2},
		"sunionstore": {Cmd: SUnionStore, Arity: -2},
		"sinter":      {Cmd: TempNotSupport, Arity: -2},
		"sinterstore": {Cmd: SInterStore, Arity: -2},
		"sdiff":       {Cmd: TempNotSupport, Arity: -2},
		"sdiffstore":  {Cmd: SDiffStore, Arity: -2},
		"smove":       {Cmd: SMove, Arity: 4},

		// zsets
		"zadd":   {Cmd: ZAdd, Arity: -4},
		"zrange": {Cmd: ZRange, Arity: -4},
		"zrem":   {Cmd: ZRem, Arity: -3},
		"zcard":  {Cmd: ZCard, Arity: 2},
	}
}
