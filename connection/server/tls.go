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

package server

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"

	"github.com/oceanbase/modis/log"
)

// TLSConfig loads the TLS certificate and key files, returning a
// tls.Config.
func TLSConfig(certFile, keyFile string) (*tls.Config, error) {
	fmt.Println("start to load TLS config...")
	log.Info("Server", nil, "start to load TLS config...")
	if certFile == "" || keyFile == "" {
		log.Warn("server", "", "cert file or key file is empty",
			log.String("cert file", certFile), log.String("key file", keyFile))
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Warn("server", nil, "fail to do LoadX509KeyPair",
			log.Errors(err), log.String("cert file", certFile), log.String("key file", keyFile))
		return nil, err
	}
	fmt.Println("load TLS config ended")
	log.Info("Server", nil, "load TLS config ended")
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		Rand:         rand.Reader,
	}, nil
}
