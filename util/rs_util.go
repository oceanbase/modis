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

package util

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/oceanbase/modis/log"
)

// TODO:(maochongxin): This method is taken from the go-TableClient and temporarily added here.
// In the future, this method should be exposed in the TableClient.
type ObHttpRslistResponse struct {
	Code int `json:"Code"`
	Cost int `json:"Cost"`
	Data struct {
		ObCluster   string `json:"ObCluster"`
		Type        string `json:"Type"`
		ObRegionId  int    `json:"ObRegionId"`
		ObClusterId int    `json:"ObClusterId"`
		RsList      []struct {
			SqlPort int    `json:"sql_port"`
			Address string `json:"address"`
			Role    string `json:"role"`
		} `json:"RsList"`
		ReadonlyRsList []string `json:"ReadonlyRsList"`
		ObRegion       string   `json:"ObRegion"`
		Timestamp      int64    `json:"timestamp"`
	} `json:"Data"`
	Message string `json:"Message"`
	Server  string `json:"Server"`
	Success bool   `json:"Success"`
	Trace   string `json:"Trace"`
}

func GetConfigServerResponseOrNull(
	url string,
	timeout time.Duration,
	retryTimes int,
	retryInternal time.Duration,
	resp *ObHttpRslistResponse) error {
	var httpResp *http.Response
	var err error
	var times int
	cli := http.Client{Timeout: timeout}
	for times = 0; times < retryTimes; times++ {
		httpResp, err = cli.Get(url)
		if err != nil {
			log.Warn("Monitor", nil, "failed to http get", log.String("url", url), log.Int("times", times))
			time.Sleep(retryInternal)
		} else {
			break
		}
	}
	if times == retryTimes {
		return fmt.Errorf("failed to http get after some retry, url:%s, times:%d", url, times)
	}
	defer func() {
		_ = httpResp.Body.Close()
	}()

	decoder := json.NewDecoder(httpResp.Body)
	for decoder.More() {
		err := decoder.Decode(resp)
		if err != nil {
			return err
		}
	}

	return nil
}

type tcpAddr struct {
	ip   string
	port int
}

type ObServerAddr struct {
	tcpAddr
	sqlPort int
}

func (a *ObServerAddr) SvrPort() int {
	return a.port
}

func (a *ObServerAddr) SqlPort() int {
	return a.sqlPort
}

func (a *ObServerAddr) Ip() string {
	return a.ip
}

type ObRslist struct {
	list []*ObServerAddr
}

func NewRslist() *ObRslist {
	return &ObRslist{
		list: make([]*ObServerAddr, 0),
	}
}
func NewObServerAddr(ip string, sqlPort int, svrPort int) *ObServerAddr {
	return &ObServerAddr{
		tcpAddr{ip, svrPort},
		sqlPort}
}

func (l *ObRslist) Append(addr *ObServerAddr) {
	l.list = append(l.list, addr)
}

func (l *ObRslist) Size() int {
	return len(l.list)
}

func (l *ObRslist) Get() *ObServerAddr {
	return l.list[0]
}

func GetTenantName(fullUserName string) string {
	utIndex := strings.Index(fullUserName, "@")
	tcIndex := strings.Index(fullUserName, "#")
	if utIndex == -1 || tcIndex == -1 || tcIndex <= utIndex {
		return ""
	}
	return fullUserName[utIndex+1 : tcIndex]
}
