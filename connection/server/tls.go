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

package server

import (
	"crypto/rand"
	"crypto/tls"

	"github.com/oceanbase/modis/log"
)

// TLSConfig loads the TLS certificate and key files, returning a
// tls.Config.
func TLSConfig(certFile, keyFile string) (*tls.Config, error) {
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
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		Rand:         rand.Reader,
	}, nil
}
